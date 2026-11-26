package k8sutils

import (
	"context"
	"errors"
	"testing"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sClientFake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

func probeableRedisPod(name, ip string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "matrix"},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			PodIP:      ip,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func notReadyRedisPod(name, ip string) *corev1.Pod {
	pod := probeableRedisPod(name, ip)
	pod.Status.Conditions[0].Status = corev1.ConditionFalse
	return pod
}

func TestIsRedisPodProbeable(t *testing.T) {
	pending := probeableRedisPod("p", "10.0.0.1")
	pending.Status.Phase = corev1.PodPending
	tests := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{name: "nil pod", pod: nil, want: false},
		{name: "running, ip, ready", pod: probeableRedisPod("p", "10.0.0.1"), want: true},
		{name: "not ready", pod: notReadyRedisPod("p", "10.0.0.1"), want: false},
		{name: "no ip", pod: probeableRedisPod("p", ""), want: false},
		{name: "pending", pod: pending, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsRedisPodProbeable(tt.pod))
		})
	}
}

func TestGetRedisReplicationTopology(t *testing.T) {
	cr := &rrvb2.RedisReplication{
		ObjectMeta: metav1.ObjectMeta{Name: "matrix-redis", Namespace: "matrix"},
		Spec:       rrvb2.RedisReplicationSpec{Size: ptr.To[int32](3)},
	}
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "matrix-redis", Namespace: "matrix"}}
	tests := []struct {
		name       string
		pods       []runtime.Object
		roles      map[string]string
		probeErrs  map[string]error
		want       RedisReplicationTopology
		wantProbed []string
	}{
		{
			name: "every pod answers",
			pods: []runtime.Object{
				probeableRedisPod("matrix-redis-0", "10.0.0.1"),
				probeableRedisPod("matrix-redis-1", "10.0.0.2"),
				probeableRedisPod("matrix-redis-2", "10.0.0.3"),
			},
			roles:      map[string]string{"matrix-redis-0": "slave", "matrix-redis-1": "master", "matrix-redis-2": "slave"},
			want:       RedisReplicationTopology{Masters: []string{"matrix-redis-1"}, Slaves: []string{"matrix-redis-0", "matrix-redis-2"}},
			wantProbed: []string{"matrix-redis-0", "matrix-redis-1", "matrix-redis-2"},
		},
		{
			name: "a not-ready pod and a missing pod are unobserved and never probed",
			pods: []runtime.Object{
				probeableRedisPod("matrix-redis-0", "10.0.0.1"),
				notReadyRedisPod("matrix-redis-1", "10.0.0.2"),
			},
			roles:      map[string]string{"matrix-redis-0": "master"},
			want:       RedisReplicationTopology{Masters: []string{"matrix-redis-0"}, Unobserved: []string{"matrix-redis-1", "matrix-redis-2"}},
			wantProbed: []string{"matrix-redis-0"},
		},
		{
			name: "a pod that does not answer or reports no role is unobserved",
			pods: []runtime.Object{
				probeableRedisPod("matrix-redis-0", "10.0.0.1"),
				probeableRedisPod("matrix-redis-1", "10.0.0.2"),
				probeableRedisPod("matrix-redis-2", "10.0.0.3"),
			},
			roles:      map[string]string{"matrix-redis-1": "", "matrix-redis-2": "slave"},
			probeErrs:  map[string]error{"matrix-redis-0": errors.New("i/o timeout")},
			want:       RedisReplicationTopology{Slaves: []string{"matrix-redis-2"}, Unobserved: []string{"matrix-redis-0", "matrix-redis-1"}},
			wantProbed: []string{"matrix-redis-0", "matrix-redis-1", "matrix-redis-2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := k8sClientFake.NewSimpleClientset(append([]runtime.Object{sts}, tt.pods...)...)
			var probed []string
			got, err := getRedisReplicationTopology(context.Background(), cl, cr, func(_ context.Context, pod *corev1.Pod) (string, error) {
				probed = append(probed, pod.Name)
				if err := tt.probeErrs[pod.Name]; err != nil {
					return "", err
				}
				return tt.roles[pod.Name], nil
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.want.Complete(), len(tt.want.Unobserved) == 0)
			assert.Equal(t, tt.wantProbed, probed)
		})
	}
}

func TestGetRedisReplicationTopologyFailsOnCancelledContext(t *testing.T) {
	cr := &rrvb2.RedisReplication{
		ObjectMeta: metav1.ObjectMeta{Name: "matrix-redis", Namespace: "matrix"},
		Spec:       rrvb2.RedisReplicationSpec{Size: ptr.To[int32](1)},
	}
	cl := k8sClientFake.NewSimpleClientset(
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "matrix-redis", Namespace: "matrix"}},
		probeableRedisPod("matrix-redis-0", "10.0.0.1"),
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := getRedisReplicationTopology(ctx, cl, cr, func(ctx context.Context, _ *corev1.Pod) (string, error) {
		return "", ctx.Err()
	})

	assert.ErrorIs(t, err, context.Canceled)
}
