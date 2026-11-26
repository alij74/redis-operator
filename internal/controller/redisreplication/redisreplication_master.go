package redisreplication

import (
	"context"
	"fmt"
	"strings"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/envs"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/k8sutils"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/service/redis"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func (r *Reconciler) redisReplicationTopology(ctx context.Context, instance *rrvb2.RedisReplication) (k8sutils.RedisReplicationTopology, error) {
	if r.RedisReplicationTopology != nil {
		return r.RedisReplicationTopology(ctx, r.K8sClient, instance)
	}
	return k8sutils.GetRedisReplicationTopology(ctx, r.K8sClient, instance)
}

func (r *Reconciler) redisReplicationRealMaster(ctx context.Context, instance *rrvb2.RedisReplication, masterPods []string) string {
	if r.RedisReplicationRealMaster != nil {
		return r.RedisReplicationRealMaster(ctx, r.K8sClient, instance, masterPods)
	}
	return k8sutils.GetRedisReplicationRealMaster(ctx, r.K8sClient, instance, masterPods)
}

func (r *Reconciler) sentinelMonitoredMaster(ctx context.Context, instance *rrvb2.RedisReplication, masterPods []string) (string, error) {
	if r.SentinelMonitoredMaster != nil {
		return r.SentinelMonitoredMaster(ctx, instance, masterPods)
	}
	return r.querySentinelMonitoredMaster(ctx, instance, masterPods)
}

func (r *Reconciler) identifyMaster(ctx context.Context, instance *rrvb2.RedisReplication, topology k8sutils.RedisReplicationTopology) string {
	if len(topology.Masters) == 0 {
		return ""
	}
	if topology.Complete() && len(topology.Masters) == 1 {
		return topology.Masters[0]
	}
	if master := r.redisReplicationRealMaster(ctx, instance, topology.Masters); master != "" {
		return master
	}
	if !instance.EnableSentinel() {
		return ""
	}
	master, err := r.sentinelMonitoredMaster(ctx, instance, topology.Masters)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed to ask sentinel which redis pod is the master")
		return ""
	}
	return master
}

func (r *Reconciler) querySentinelMonitoredMaster(ctx context.Context, instance *rrvb2.RedisReplication, masterPods []string) (string, error) {
	sentinelPods, err := r.getSentinelPods(ctx, instance)
	if err != nil {
		return "", err
	}
	password, err := r.sentinelPassword(ctx, instance)
	if err != nil {
		return "", err
	}
	votes := map[string]int{}
	redisClient := redis.NewClient()
	for _, pod := range sentinelPods.Items {
		if !k8sutils.IsRedisPodProbeable(&pod) {
			continue
		}
		info, err := redisClient.Connect(&redis.ConnectionInfo{
			Host:     pod.Status.PodIP,
			Port:     "26379",
			Password: password,
		}).GetInfoSentinel(ctx)
		if err != nil || info == nil {
			continue
		}
		for _, master := range info.Masters {
			if master.Name == masterGroupName {
				votes[addressHost(master.Address)]++
			}
		}
	}
	quorum := int(instance.Spec.Sentinel.Size/2) + 1
	for _, podName := range masterPods {
		pod, err := r.K8sClient.CoreV1().Pods(instance.Namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			continue
		}
		fqdn := fmt.Sprintf("%s.%s.%s.svc.%s", podName, common.GetHeadlessServiceNameFromPodName(podName), instance.Namespace, envs.GetServiceDNSDomain())
		if votes[pod.Status.PodIP] >= quorum || votes[fqdn] >= quorum {
			return podName, nil
		}
	}
	return "", nil
}

func (r *Reconciler) sentinelPassword(ctx context.Context, instance *rrvb2.RedisReplication) (string, error) {
	if instance.Spec.Sentinel.ExistingPasswordSecret == nil {
		return "", nil
	}
	secret, err := r.K8sClient.CoreV1().Secrets(instance.Namespace).Get(
		ctx,
		*instance.Spec.Sentinel.ExistingPasswordSecret.Name,
		metav1.GetOptions{},
	)
	if err != nil {
		return "", err
	}
	return string(secret.Data[*instance.Spec.Sentinel.ExistingPasswordSecret.Key]), nil
}

func addressHost(address string) string {
	if i := strings.LastIndex(address, ":"); i >= 0 {
		return address[:i]
	}
	return address
}
