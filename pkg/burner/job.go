// Copyright 2020 The Kube-burner Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package burner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/cloud-bulldozer/go-commons/indexers"
	"github.com/cloud-bulldozer/go-commons/version"
	"github.com/kube-burner/kube-burner/pkg/config"
	"github.com/kube-burner/kube-burner/pkg/measurements"
	"github.com/kube-burner/kube-burner/pkg/prometheus"
	"github.com/kube-burner/kube-burner/pkg/util"
	"github.com/kube-burner/kube-burner/pkg/util/metrics"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/rest"
)

// returnPair is a pair of return codes for a job
type returnPair struct {
	innerRC         int
	executionErrors string
}

const (
	jobName              = "JobName"
	replica              = "Replica"
	jobIteration         = "Iteration"
	jobUUID              = "UUID"
	rcTimeout            = 2
	rcAlert              = 3
	rcMeasurement        = 4
	garbageCollectionJob = "garbage-collection"
)

var (
	supportedExecutionMode = map[config.ExecutionMode]struct{}{
		config.ExecutionModeParallel:   {},
		config.ExecutionModeSequential: {},
	}
)

// Runs the with the given configuration and metrics scraper, with the specified timeout.
// Returns:
// - error code
// - error
//
//nolint:gocyclo
func Run(configSpec config.Spec, kubeClientProvider *config.KubeClientProvider, metricsScraper metrics.Scraper, timeout time.Duration) (int, error) {
	var err error
	var rc int
	var executedJobs []prometheus.Job
	var msWg, gcWg sync.WaitGroup
	errs := []error{}
	res := make(chan int, 1)
	uuid := configSpec.GlobalConfig.UUID
	globalConfig := configSpec.GlobalConfig
	globalWaitMap := make(map[string][]string)
	executorMap := make(map[string]*Executor)
	returnMap := make(map[string]returnPair)
	log.Infof("🔥 Starting kube-burner (%s@%s) with UUID %s", version.Version, version.GitCommit, uuid)
	go func() {
		var innerRC int
		measurements.NewMeasurementFactory(configSpec, metricsScraper.MetricsMetadata)
		executorList := newExecutorList(configSpec, kubeClientProvider, timeout)
		handlePreloadImages(executorList, kubeClientProvider)
		// Iterate job list
		for index, executor := range executorList {
			var waitListNamespaces []string
			currentJob := prometheus.Job{
				Start:     time.Now().UTC(),
				JobConfig: executor.Job,
			}
			measurements.SetJobConfig(&executor.Job, kubeClientProvider)
			log.Infof("Triggering job: %s", executor.Name)
			measurements.Start()
			if executor.JobType == config.CreationJob {
				if executor.Cleanup {
					// No timeout for initial job cleanup
					garbageCollectJob(context.TODO(), executor, fmt.Sprintf("kube-burner-job=%s", executor.Name), nil)
				}
				if executor.Churn {
					log.Info("Churning enabled")
					log.Infof("Churn cycles: %v", executor.ChurnCycles)
					log.Infof("Churn duration: %v", executor.ChurnDuration)
					log.Infof("Churn percent: %v", executor.ChurnPercent)
					log.Infof("Churn delay: %v", executor.ChurnDelay)
					log.Infof("Churn deletion strategy: %v", executor.ChurnDeletionStrategy)
				}
				executor.RunCreateJob(0, executor.JobIterations, &waitListNamespaces)
				// If object verification is enabled
				if executor.VerifyObjects && !executor.Verify() {
					err := errors.New("object verification failed")
					// If errorOnVerify is enabled. Set RC to 1 and append error
					if executor.ErrorOnVerify {
						innerRC = 1
						errs = append(errs, err)
					}
					log.Error(err.Error())
				}
				if executor.Churn {
					now := time.Now().UTC()
					currentJob.ChurnStart = &now
					executor.RunCreateJobWithChurn()
					currentJob.ChurnEnd = &now
				}
				globalWaitMap[strconv.Itoa(index)+executor.Name] = waitListNamespaces
				executorMap[strconv.Itoa(index)+executor.Name] = executor
			} else {
				executor.RunJob()
			}
			if executor.BeforeCleanup != "" {
				log.Infof("Waiting for beforeCleanup command %s to finish", executor.BeforeCleanup)
				cmd := exec.Command("/bin/sh", executor.BeforeCleanup)
				var outb, errb bytes.Buffer
				cmd.Stdout = &outb
				cmd.Stderr = &errb
				err := cmd.Run()
				if err != nil {
					err = fmt.Errorf("BeforeCleanup failed: %v", err)
					log.Error(err.Error())
					errs = append(errs, err)
					innerRC = 1
				}
				log.Infof("BeforeCleanup out: %v, err: %v", outb.String(), errb.String())
			}
			if executor.JobPause > 0 {
				log.Infof("Pausing for %v before finishing job", executor.JobPause)
				time.Sleep(executor.JobPause)
			}
			currentJob.End = time.Now().UTC()
			executedJobs = append(executedJobs, currentJob)
			if !globalConfig.WaitWhenFinished {
				elapsedTime := currentJob.End.Sub(currentJob.Start).Round(time.Second)
				log.Infof("Job %s took %v", executor.Name, elapsedTime)
			}
			// We stop and index measurements per job
			if err = measurements.Stop(); err != nil {
				errs = append(errs, err)
				log.Error(err.Error())
				innerRC = rcMeasurement
			}
			if !executor.SkipIndexing && len(metricsScraper.IndexerList) > 0 {
				msWg.Add(1)
				go func(jobName string) {
					defer msWg.Done()
					measurements.Index(jobName, metricsScraper.IndexerList)
				}(executor.Name)
			}
		}
		if globalConfig.WaitWhenFinished {
			runWaitList(globalWaitMap, executorMap)
		}
		// We initialize garbage collection as soon as the benchmark finishes
		if globalConfig.GC {
			//nolint:govet
			gcCtx, _ := context.WithTimeout(context.Background(), globalConfig.GCTimeout)
			for _, executor := range executorList {
				gcWg.Add(1)
				go garbageCollectJob(gcCtx, executor, fmt.Sprintf("kube-burner-job=%s", executor.Name), &gcWg)
			}
			if globalConfig.GCMetrics {
				cleanupStart := time.Now().UTC()
				log.Info("Garbage collection metrics on, waiting for GC")
				// If gcMetrics is enabled, garbage collection must be blocker
				gcWg.Wait()
				// We add an extra dummy job to executedJobs to index metrics from this stage
				executedJobs = append(executedJobs, prometheus.Job{
					Start: cleanupStart,
					End:   time.Now().UTC(),
					JobConfig: config.Job{
						Name: garbageCollectionJob,
					},
				})
			}
		}
		// Make sure that measurements have indexed their stuff before we index metrics
		msWg.Wait()
		for _, job := range executedJobs {
			// Declare slice on each iteration
			var jobAlerts []error
			var executionErrors string
			for _, alertM := range metricsScraper.AlertMs {
				if err := alertM.Evaluate(job); err != nil {
					errs = append(errs, err)
					jobAlerts = append(jobAlerts, err)
					innerRC = rcAlert
				}
			}
			if len(jobAlerts) > 0 {
				executionErrors = utilerrors.NewAggregate(jobAlerts).Error()
			}
			returnMap[job.JobConfig.Name] = returnPair{innerRC: innerRC, executionErrors: executionErrors}
		}
		indexMetrics(uuid, executedJobs, returnMap, metricsScraper, configSpec, true, "", false)
		log.Infof("Finished execution with UUID: %s", uuid)
		res <- innerRC
	}()
	select {
	case rc = <-res:
	case <-time.After(timeout):
		err := fmt.Errorf("%v timeout reached", timeout)
		log.Error(err.Error())
		errs = append(errs, err)
		rc = rcTimeout
		indexMetrics(uuid, executedJobs, returnMap, metricsScraper, configSpec, false, utilerrors.NewAggregate(errs).Error(), true)
	}
	// When GC is enabled and GCMetrics is disabled, we assume previous GC operation ran in background, so we have to ensure there's no garbage left
	if globalConfig.GC && !globalConfig.GCMetrics {
		log.Info("Garbage collecting jobs")
		gcWg.Wait()
	}
	return rc, utilerrors.NewAggregate(errs)
}

// If requests, preload the images used in the test into the node
func handlePreloadImages(executorList []*Executor, kubeClientProvider *config.KubeClientProvider) {
	clientSet, _ := kubeClientProvider.DefaultClientSet()
	for _, executor := range executorList {
		if executor.PreLoadImages && executor.JobType == config.CreationJob {
			if err := preLoadImages(executor, clientSet); err != nil {
				log.Fatal(err.Error())
			}
		}
	}
}

// indexMetrics indexes metrics for the executed jobs
func indexMetrics(uuid string, executedJobs []prometheus.Job, returnMap map[string]returnPair, metricsScraper metrics.Scraper, configSpec config.Spec, innerRC bool, executionErrors string, isTimeout bool) {
	var jobSummaries []JobSummary
	for _, job := range executedJobs {
		if !job.JobConfig.SkipIndexing {
			if value, exists := returnMap[job.JobConfig.Name]; exists && !isTimeout {
				innerRC = value.innerRC == 0
				executionErrors = value.executionErrors
			}
			jobSummaries = append(jobSummaries, JobSummary{
				UUID:                uuid,
				Timestamp:           job.Start,
				EndTimestamp:        job.End,
				ElapsedTime:         job.End.Sub(job.Start).Round(time.Second).Seconds(),
				ChurnStartTimestamp: job.ChurnStart,
				ChurnEndTimestamp:   job.ChurnEnd,
				JobConfig:           job.JobConfig,
				Metadata:            metricsScraper.SummaryMetadata,
				Passed:              innerRC,
				ExecutionErrors:     executionErrors,
				Version:             fmt.Sprintf("%v@%v", version.Version, version.GitCommit),
				MetricName:          jobSummaryMetric,
			})
		}
	}
	for _, indexer := range metricsScraper.IndexerList {
		IndexJobSummary(jobSummaries, indexer)
	}
	for _, prometheusClient := range metricsScraper.PrometheusClients {
		prometheusClient.ScrapeJobsMetrics(executedJobs...)
	}
	for _, indexer := range configSpec.MetricsEndpoints {
		if indexer.IndexerConfig.Type == indexers.LocalIndexer && indexer.IndexerConfig.CreateTarball {
			metrics.CreateTarball(indexer.IndexerConfig)
		}
	}
}

func verifyJobTimeout(job *config.Job, defaultTimeout time.Duration) {
	if job.MaxWaitTimeout == 0 {
		log.Debugf("job.MaxWaitTimeout is zero in %s, override by timeout: %s", job.Name, defaultTimeout)
		job.MaxWaitTimeout = defaultTimeout
	} else {
		log.Debugf("job.MaxWaitTimeout is non zero in %s: %s", job.Name, job.MaxWaitTimeout)
	}
}

func verifyQPSBurst(job *config.Job) {
	if job.QPS == 0 || job.Burst == 0 {
		log.Infof("QPS or Burst rates not set, using default client-go values: %v %v", rest.DefaultQPS, rest.DefaultBurst)
		job.QPS = rest.DefaultQPS
		job.Burst = rest.DefaultBurst
	} else {
		log.Infof("QPS: %v", job.QPS)
		log.Infof("Burst: %v", job.Burst)
	}
}

func verifyJobDefaults(job *config.Job, defaultTimeout time.Duration) {
	verifyJobTimeout(job, defaultTimeout)
	verifyQPSBurst(job)
}

// newExecutorList Returns a list of executors
func newExecutorList(configSpec config.Spec, kubeClientProvider *config.KubeClientProvider, defaultTimeout time.Duration) []*Executor {
	var executorList []*Executor
	for _, job := range configSpec.Jobs {
		verifyJobDefaults(&job, defaultTimeout)
		executorList = append(executorList, newExecutor(configSpec, kubeClientProvider, job))
	}
	return executorList
}

// Runs on wait list at the end of benchmark
func runWaitList(globalWaitMap map[string][]string, executorMap map[string]*Executor) {
	var wg sync.WaitGroup
	for executorUUID, namespaces := range globalWaitMap {
		executor := executorMap[executorUUID]
		log.Infof("Waiting up to %s for actions to be completed", executor.MaxWaitTimeout)
		// This semaphore is used to limit the maximum number of concurrent goroutines
		sem := make(chan int, int(executor.restConfig.QPS))
		for _, ns := range namespaces {
			sem <- 1
			wg.Add(1)
			go func(ns string) {
				executor.waitForObjects(ns, rate.NewLimiter(rate.Limit(executor.restConfig.QPS), executor.restConfig.Burst))
				<-sem
				wg.Done()
			}(ns)
		}
		wg.Wait()
	}
}

func garbageCollectJob(ctx context.Context, jobExecutor *Executor, labelSelector string, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	util.CleanupNamespaces(ctx, jobExecutor.clientSet, labelSelector)
	for _, obj := range jobExecutor.objects {
		jobExecutor.limiter.Wait(ctx)
		if !obj.Namespaced {
			CleanupNonNamespacedResourcesUsingGVR(ctx, jobExecutor, obj, labelSelector)
		} else if obj.namespace != "" { // When the object has a fixed namespace not generated by kube-burner
			CleanupNamespaceResourcesUsingGVR(ctx, jobExecutor, obj, obj.namespace, labelSelector)
		}
	}
}
