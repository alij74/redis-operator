package redis

import (
	"context"
	"testing"

	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sClientFake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

var replicationSelector = map[string]string{"app": "matrix-redis", "redis_setup_type": "replication", "role": "replication"}

func replicationPod(name, role string, ready bool) *corev1.Pod {
	labels := map[string]string{}
	for key, value := range replicationSelector {
		labels[key] = value
	}
	if role != "" {
		labels[common.RedisRoleLabelKey] = role
	}
	condition := corev1.ConditionTrue
	if !ready {
		condition = corev1.ConditionFalse
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "matrix", Labels: labels},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			PodIP:      "10.0.0.1",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: condition}},
		},
	}
}

func roleLabel(t *testing.T, cl *k8sClientFake.Clientset, name string) string {
	t.Helper()
	pod, err := cl.CoreV1().Pods("matrix").Get(context.Background(), name, metav1.GetOptions{})
	require.NoError(t, err)
	return pod.Labels[common.RedisRoleLabelKey]
}

func patchedPods(cl *k8sClientFake.Clientset) []string {
	var names []string
	for _, action := range cl.Actions() {
		if patch, ok := action.(k8stesting.PatchAction); ok {
			names = append(names, patch.GetName())
		}
	}
	return names
}

func TestUpdateRedisReplicationRoleLabelsFollowsTheIdentifiedMaster(t *testing.T) {
	cl := k8sClientFake.NewSimpleClientset(
		replicationPod("matrix-redis-0", common.RedisRoleLabelSlave, true),
		replicationPod("matrix-redis-1", common.RedisRoleLabelMaster, false),
		replicationPod("matrix-redis-2", "", true),
	)
	h := &healer{k8s: cl}

	require.NoError(t, h.UpdateRedisReplicationRoleLabels(context.Background(), "matrix", replicationSelector, "matrix-redis-0"))

	assert.Equal(t, common.RedisRoleLabelMaster, roleLabel(t, cl, "matrix-redis-0"))
	assert.Equal(t, common.RedisRoleLabelSlave, roleLabel(t, cl, "matrix-redis-1"))
	assert.Equal(t, common.RedisRoleLabelSlave, roleLabel(t, cl, "matrix-redis-2"))
	assert.Equal(t, []string{"matrix-redis-1", "matrix-redis-2", "matrix-redis-0"}, patchedPods(cl))
}

func TestUpdateRedisReplicationRoleLabelsPatchesNothingInSteadyState(t *testing.T) {
	cl := k8sClientFake.NewSimpleClientset(
		replicationPod("matrix-redis-0", common.RedisRoleLabelMaster, true),
		replicationPod("matrix-redis-1", common.RedisRoleLabelSlave, true),
	)
	h := &healer{k8s: cl}

	require.NoError(t, h.UpdateRedisReplicationRoleLabels(context.Background(), "matrix", replicationSelector, "matrix-redis-0"))

	assert.Empty(t, patchedPods(cl))
}

func TestUpdateRedisReplicationRoleLabelsChangesNothingWithoutAKnownMaster(t *testing.T) {
	for _, masterPod := range []string{"", "matrix-redis-9"} {
		t.Run("master="+masterPod, func(t *testing.T) {
			cl := k8sClientFake.NewSimpleClientset(
				replicationPod("matrix-redis-0", common.RedisRoleLabelSlave, true),
				replicationPod("matrix-redis-1", common.RedisRoleLabelMaster, true),
			)
			h := &healer{k8s: cl}

			require.NoError(t, h.UpdateRedisReplicationRoleLabels(context.Background(), "matrix", replicationSelector, masterPod))

			assert.Empty(t, patchedPods(cl))
			assert.Equal(t, common.RedisRoleLabelMaster, roleLabel(t, cl, "matrix-redis-1"))
		})
	}
}

func TestUpdateRedisReplicationRoleLabelsToleratesAPodDeletedMeanwhile(t *testing.T) {
	cl := k8sClientFake.NewSimpleClientset(
		replicationPod("matrix-redis-0", common.RedisRoleLabelSlave, true),
		replicationPod("matrix-redis-1", common.RedisRoleLabelMaster, false),
	)
	cl.PrependReactor("patch", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.(k8stesting.PatchAction).GetName() == "matrix-redis-1" {
			return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "matrix-redis-1")
		}
		return false, nil, nil
	})
	h := &healer{k8s: cl}

	require.NoError(t, h.UpdateRedisReplicationRoleLabels(context.Background(), "matrix", replicationSelector, "matrix-redis-0"))

	assert.Equal(t, common.RedisRoleLabelMaster, roleLabel(t, cl, "matrix-redis-0"))
}
