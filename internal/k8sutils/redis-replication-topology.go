package k8sutils

import (
	"context"
	"strconv"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type RedisReplicationTopology struct {
	Masters    []string
	Slaves     []string
	Unobserved []string
}

func (t RedisReplicationTopology) Complete() bool {
	return len(t.Unobserved) == 0
}

func GetRedisReplicationTopology(ctx context.Context, cl kubernetes.Interface, cr *rrvb2.RedisReplication) (RedisReplicationTopology, error) {
	return getRedisReplicationTopology(ctx, cl, cr, func(ctx context.Context, pod *corev1.Pod) (string, error) {
		redisClient := configureRedisReplicationClient(ctx, cl, cr, pod.Name)
		defer redisClient.Close()
		return checkRedisServerRole(ctx, redisClient, pod.Name)
	})
}

func getRedisReplicationTopology(ctx context.Context, cl kubernetes.Interface, cr *rrvb2.RedisReplication, probeRole func(context.Context, *corev1.Pod) (string, error)) (RedisReplicationTopology, error) {
	statefulset, err := GetStatefulSet(ctx, cl, cr.GetNamespace(), cr.GetName())
	if err != nil {
		return RedisReplicationTopology{}, err
	}

	var topology RedisReplicationTopology
	replicas := cr.Spec.GetReplicationCounts("replication")
	for i := 0; i < int(replicas); i++ {
		podName := statefulset.Name + "-" + strconv.Itoa(i)
		pod, err := cl.CoreV1().Pods(cr.Namespace).Get(ctx, podName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			topology.Unobserved = append(topology.Unobserved, podName)
			continue
		}
		if err != nil {
			return RedisReplicationTopology{}, err
		}
		if !IsRedisPodProbeable(pod) {
			topology.Unobserved = append(topology.Unobserved, podName)
			continue
		}
		role, err := probeRole(ctx, pod)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return RedisReplicationTopology{}, ctxErr
			}
			topology.Unobserved = append(topology.Unobserved, podName)
			continue
		}
		switch role {
		case "master":
			topology.Masters = append(topology.Masters, podName)
		case "slave":
			topology.Slaves = append(topology.Slaves, podName)
		default:
			topology.Unobserved = append(topology.Unobserved, podName)
		}
	}
	return topology, nil
}

func IsRedisPodProbeable(pod *corev1.Pod) bool {
	if pod == nil || pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
