/*
Copyright 2020 Opstree Solutions.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"strconv"
	"time"

	"github.com/OT-CONTAINER-KIT/redis-operator/k8sutils"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1beta1 "github.com/OT-CONTAINER-KIT/redis-operator/api/v1beta1"
)

// RedisClusterReconciler reconciles a RedisCluster object
type RedisClusterReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// Reconcile is part of the main kubernetes reconciliation loop
func (r *RedisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reqLogger := r.Log.WithValues("Request.Namespace", req.Namespace, "Request.Name", req.Name)
	reqLogger.Info("Reconciling opstree redis Cluster controller")
	instance := &redisv1beta1.RedisCluster{}

	// 这里的数据应该是从Informer中获取到
	err := r.Client.Get(context.TODO(), req.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 打了这个注解，就直接忽略
	if _, found := instance.ObjectMeta.GetAnnotations()["rediscluster.opstreelabs.in/skip-reconcile"]; found {
		reqLogger.Info("Found annotations rediscluster.opstreelabs.in/skip-reconcile, so skipping reconcile")
		return ctrl.Result{RequeueAfter: time.Second * 10}, nil
	}

	// 没指定leader的话，默认就是size大小
	leaderReplicas := instance.Spec.GetReplicaCounts("leader")
	// 没指定leader的话，默认就是size大小
	followerReplicas := instance.Spec.GetReplicaCounts("follower")
	// 如果spec.Size=3，那么这里的总共副本数就是6
	totalReplicas := leaderReplicas + followerReplicas

	// 如果设置了Finalizer并且删除了RedisCluster，就需要做清理工作，这里主要是删除了pvc，statefulset以及service并没有删除
	// 另外这里的PVC有两种，一种是普通的，另外一种是为了保存node-conf而创建的PVC文件
	if err := k8sutils.HandleRedisClusterFinalizer(instance, r.Client); err != nil {
		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	}

	// 如果当前RedisCluster没有加入Finalizer，那么增加Finalizer
	if err := k8sutils.AddRedisClusterFinalizer(instance, r.Client); err != nil {
		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	}

	// Check if the cluster is downscaled
	// 通过cluster nodes命令的执行结果检查当前集群有几个leader节点
	if leaderReplicas < k8sutils.CheckRedisNodeCount(instance, "leader") {

		//  Imp if the last index of leader sts is not leader make it then
		// check whether the redis is leader or not ?
		// if not true then make it leader pod

		// TODO 如果主从切换了，通过这一段代码就能检测到，这里我感觉所有的Leader节点都判断会合适一些
		if !(k8sutils.VerifyLeaderPod(instance)) {
			// lastLeaderPod is slaving right now Make it the master Pod
			// We have to bring a manual failover here to make it a leaderPod
			// clusterFailover should also include the clusterReplicate since we have to map the followers to new leader
			// 执行cluster failover命令
			k8sutils.ClusterFailover(instance)
		}

		// Step 1 Rehard the Cluster
		k8sutils.ReshardRedisCluster(instance)
		// Step 2 Remove the Follower Node
		k8sutils.RemoveRedisFollowerNodesFromCluster(instance)
		// Step 3 Remove the Leader Node
		k8sutils.RemoveRedisNodeFromCluster(instance)
		// Step 4 Rebalance the cluster
		k8sutils.RebalanceRedisCluster(instance)
		return ctrl.Result{RequeueAfter: time.Second * 100}, nil
	}

	// 创建Leader StatefulSet，如果已经存在，就对比出前后差异，看看是否需要更新
	// TODO 这里面的对比更新逻辑值得好好学习一下
	err = k8sutils.CreateRedisLeader(instance)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	}
	if leaderReplicas != 0 {
		// 创建leader service
		err = k8sutils.CreateRedisLeaderService(instance)
		if err != nil {
			return ctrl.Result{RequeueAfter: time.Second * 60}, err
		}
	}

	// 检查PDB
	err = k8sutils.ReconcileRedisPodDisruptionBudget(instance, "leader", instance.Spec.RedisLeader.PodDisruptionBudget)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	}

	// 获取leader StatefulSet
	redisLeaderInfo, err := k8sutils.GetStatefulSet(instance.Namespace, instance.ObjectMeta.Name+"-leader")
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	}

	// 如果Leader就绪了，就创建follower
	if int32(redisLeaderInfo.Status.ReadyReplicas) == leaderReplicas {
		err = k8sutils.CreateRedisFollower(instance)
		if err != nil {
			return ctrl.Result{RequeueAfter: time.Second * 60}, err
		}
		// if we have followers create their service.
		if followerReplicas != 0 {
			err = k8sutils.CreateRedisFollowerService(instance)
			if err != nil {
				return ctrl.Result{RequeueAfter: time.Second * 60}, err
			}
		}
		err = k8sutils.ReconcileRedisPodDisruptionBudget(instance, "follower", instance.Spec.RedisFollower.PodDisruptionBudget)
		if err != nil {
			return ctrl.Result{RequeueAfter: time.Second * 60}, err
		}
	}
	redisFollowerInfo, err := k8sutils.GetStatefulSet(instance.Namespace, instance.ObjectMeta.Name+"-follower")
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	}

	if leaderReplicas == 0 {
		reqLogger.Info("Redis leaders Cannot be 0", "Ready.Replicas", strconv.Itoa(int(redisLeaderInfo.Status.ReadyReplicas)), "Expected.Replicas", leaderReplicas)
		return ctrl.Result{RequeueAfter: time.Second * 120}, nil
	}

	if int32(redisLeaderInfo.Status.ReadyReplicas) != leaderReplicas && int32(redisFollowerInfo.Status.ReadyReplicas) != followerReplicas {
		reqLogger.Info("Redis leader and follower nodes are not ready yet", "Ready.Replicas", strconv.Itoa(int(redisLeaderInfo.Status.ReadyReplicas)), "Expected.Replicas", leaderReplicas)
		return ctrl.Result{RequeueAfter: time.Second * 120}, nil
	}
	reqLogger.Info("Creating redis cluster by executing cluster creation commands", "Leaders.Ready", strconv.Itoa(int(redisLeaderInfo.Status.ReadyReplicas)), "Followers.Ready", strconv.Itoa(int(redisFollowerInfo.Status.ReadyReplicas)))

	// 执行cluster nodes命令，检查总共有几个节点，如果拿到的节点数量和期望总数不相等，说明集群组建还有问题
	if k8sutils.CheckRedisNodeCount(instance, "") != totalReplicas {
		// 通过执行cluster nodes命令，获取leader数量
		leaderCount := k8sutils.CheckRedisNodeCount(instance, "leader")
		// 判断leader节点数量是否和预期数量相等
		if leaderCount != leaderReplicas {
			reqLogger.Info("Not all leader are part of the cluster...", "Leaders.Count", leaderCount, "Instance.Size", leaderReplicas)
			if leaderCount <= 2 {
				// 执行此命令组建集群：redis-cli --cluster create 10.233.97.157:6379 10.233.75.69:6379 10.233.74.65:6379 --cluster-yes -a <password>
				k8sutils.ExecuteRedisClusterCommand(instance)
			} else {
				if leaderCount < leaderReplicas {
					// Scale up the cluster
					// Step 2 : Add Redis Node
					// 执行命令，把新的节点加入到leader-0中：redis-cli --cluster add-node 10.233.74.89:6379 10.233.74.110:6379 --cluster-slave -a <password>
					k8sutils.AddRedisNodeToCluster(instance)
					// Step 3 Rebalance the cluster using the empty masters
					// 执行：redis-cli --cluster rebalance <redis>:<port> --cluster-use-empty-masters -a <pass>
					k8sutils.RebalanceRedisClusterEmptyMasters(instance)
				}
			}
		} else {
			if followerReplicas > 0 && redisFollowerInfo.Status.ReadyReplicas == followerReplicas {
				reqLogger.Info("All leader are part of the cluster, adding follower/replicas", "Leaders.Count", leaderCount, "Instance.Size", leaderReplicas, "Follower.Replicas", followerReplicas)
				k8sutils.ExecuteRedisReplicationCommand(instance)
			} else {
				reqLogger.Info("no follower/replicas configured, skipping replication configuration", "Leaders.Count", leaderCount, "Leader.Size", leaderReplicas, "Follower.Replicas", followerReplicas)
			}
		}
	} else {
		reqLogger.Info("Redis leader count is desired")
		if int(totalReplicas) > 1 && k8sutils.CheckRedisClusterState(instance) >= int(totalReplicas)-1 {
			reqLogger.Info("Redis leader is not desired, executing failover operation")
			err = k8sutils.ExecuteFailoverOperation(instance)
			if err != nil {
				return ctrl.Result{RequeueAfter: time.Second * 10}, err
			}
		}
		return ctrl.Result{RequeueAfter: time.Second * 120}, nil
	}

	// Check If there is No Empty Master Node
	// 判断Master节点分配的Slot是否为空，如果为空就重新分配Slot
	if k8sutils.CheckRedisNodeCount(instance, "") == totalReplicas {
		k8sutils.CheckIfEmptyMasters(instance)
	}
	reqLogger.Info("Will reconcile redis cluster operator in again 10 seconds")
	return ctrl.Result{RequeueAfter: time.Second * 10}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redisv1beta1.RedisCluster{}).
		Complete(r)
}
