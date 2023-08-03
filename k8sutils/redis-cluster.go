package k8sutils

import (
	redisv1beta1 "github.com/OT-CONTAINER-KIT/redis-operator/api/v1beta1"

	corev1 "k8s.io/api/core/v1"
)

// RedisClusterSTS is a interface to call Redis Statefulset function
type RedisClusterSTS struct {
	// redis的角色类型，要么是follower，要么是leader
	RedisStateFulType string
	// redis的配置文件，这个值是configmap的名字
	ExternalConfig                *string
	SecurityContext               *corev1.SecurityContext
	Affinity                      *corev1.Affinity `json:"affinity,omitempty"`
	TerminationGracePeriodSeconds *int64           `json:"terminationGracePeriodSeconds,omitempty" protobuf:"varint,4,opt,name=terminationGracePeriodSeconds"`
	ReadinessProbe                *redisv1beta1.Probe
	LivenessProbe                 *redisv1beta1.Probe
	NodeSelector                  map[string]string
	Tolerations                   *[]corev1.Toleration
}

// RedisClusterService is a interface to call Redis Service function
type RedisClusterService struct {
	RedisServiceRole string
}

// generateRedisClusterParams generates Redis cluster information
func generateRedisClusterParams(cr *redisv1beta1.RedisCluster, replicas int32, externalConfig *string, params RedisClusterSTS) statefulSetParameters {
	res := statefulSetParameters{
		Metadata:                      cr.ObjectMeta, // metadata元数据
		Replicas:                      &replicas,     // 副本数
		ClusterMode:                   true,
		NodeSelector:                  params.NodeSelector,
		PodSecurityContext:            cr.Spec.PodSecurityContext,
		PriorityClassName:             cr.Spec.PriorityClassName,
		Affinity:                      params.Affinity,
		TerminationGracePeriodSeconds: params.TerminationGracePeriodSeconds,
		Tolerations:                   params.Tolerations,
		ServiceAccountName:            cr.Spec.ServiceAccountName,
		UpdateStrategy:                cr.Spec.KubernetesConfig.UpdateStrategy,
	}
	// 暴露指标
	if cr.Spec.RedisExporter != nil {
		res.EnableMetrics = cr.Spec.RedisExporter.Enabled
	}
	// 私有镜像的拉取的用户名密码
	if cr.Spec.KubernetesConfig.ImagePullSecrets != nil {
		res.ImagePullSecrets = cr.Spec.KubernetesConfig.ImagePullSecrets
	}
	// redis持久化存储
	if cr.Spec.Storage != nil {
		// 这个存储是给redis存放数据用的，也就是aof, rdb文件
		res.PersistentVolumeClaim = cr.Spec.Storage.VolumeClaimTemplate
		// 这个存储是给nodes.conf文件用的，这个文件记录了集群信息
		res.NodeConfPersistentVolumeClaim = cr.Spec.Storage.NodeConfVolumeClaimTemplate
	}
	// redis的配置，这个值是configmap的名字
	if externalConfig != nil {
		res.ExternalConfig = externalConfig
	}
	// 如果发现了这个注解，标识statefulset需要重新创建
	if _, found := cr.ObjectMeta.GetAnnotations()["redis.opstreelabs.in/recreate-statefulset"]; found {
		res.RecreateStatefulSet = true
	}
	return res
}

func generateRedisClusterInitContainerParams(cr *redisv1beta1.RedisCluster) initContainerParameters {
	trueProperty := true
	initcontainerProp := initContainerParameters{}

	if cr.Spec.InitContainer != nil {
		initContainer := cr.Spec.InitContainer

		initcontainerProp = initContainerParameters{
			Enabled:               initContainer.Enabled,
			Role:                  "cluster",
			Image:                 initContainer.Image,
			ImagePullPolicy:       initContainer.ImagePullPolicy,
			Resources:             initContainer.Resources,
			AdditionalEnvVariable: initContainer.EnvVars,
			Command:               initContainer.Command,
			Arguments:             initContainer.Args,
		}

		if cr.Spec.Storage != nil {
			initcontainerProp.AdditionalVolume = cr.Spec.Storage.VolumeMount.Volume
			initcontainerProp.AdditionalMountPath = cr.Spec.Storage.VolumeMount.MountPath
		}
		if cr.Spec.Storage != nil {
			initcontainerProp.PersistenceEnabled = &trueProperty
		}

	}

	return initcontainerProp
}

// generateRedisClusterContainerParams generates Redis container information
func generateRedisClusterContainerParams(cr *redisv1beta1.RedisCluster, securityContext *corev1.SecurityContext, readinessProbeDef *redisv1beta1.Probe, livenessProbeDef *redisv1beta1.Probe) containerParameters {
	trueProperty := true
	falseProperty := false
	containerProp := containerParameters{
		Role:            "cluster",
		Image:           cr.Spec.KubernetesConfig.Image,
		ImagePullPolicy: cr.Spec.KubernetesConfig.ImagePullPolicy,
		Resources:       cr.Spec.KubernetesConfig.Resources,
		SecurityContext: securityContext,
	}
	if cr.Spec.Storage != nil {
		containerProp.AdditionalVolume = cr.Spec.Storage.VolumeMount.Volume
		containerProp.AdditionalMountPath = cr.Spec.Storage.VolumeMount.MountPath
	}
	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		containerProp.EnabledPassword = &trueProperty
		containerProp.SecretName = cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name
		containerProp.SecretKey = cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key
	} else {
		containerProp.EnabledPassword = &falseProperty
	}
	if cr.Spec.RedisExporter != nil {
		containerProp.RedisExporterImage = cr.Spec.RedisExporter.Image
		containerProp.RedisExporterImagePullPolicy = cr.Spec.RedisExporter.ImagePullPolicy

		if cr.Spec.RedisExporter.Resources != nil {
			containerProp.RedisExporterResources = cr.Spec.RedisExporter.Resources
		}

		if cr.Spec.RedisExporter.EnvVars != nil {
			containerProp.RedisExporterEnv = cr.Spec.RedisExporter.EnvVars
		}

	}
	if readinessProbeDef != nil {
		containerProp.ReadinessProbe = readinessProbeDef
	}
	if livenessProbeDef != nil {
		containerProp.LivenessProbe = livenessProbeDef
	}
	if cr.Spec.Storage != nil && cr.Spec.PersistenceEnabled != nil && *cr.Spec.PersistenceEnabled {
		containerProp.PersistenceEnabled = &trueProperty
	} else {
		containerProp.PersistenceEnabled = &falseProperty
	}
	if cr.Spec.TLS != nil {
		containerProp.TLSConfig = cr.Spec.TLS
	}
	if cr.Spec.ACL != nil {
		containerProp.ACLConfig = cr.Spec.ACL
	}

	return containerProp
}

// CreateRedisLeader will create a leader redis setup
func CreateRedisLeader(cr *redisv1beta1.RedisCluster) error {
	prop := RedisClusterSTS{
		RedisStateFulType:             "leader",
		SecurityContext:               cr.Spec.RedisLeader.SecurityContext,
		Affinity:                      cr.Spec.RedisLeader.Affinity,
		TerminationGracePeriodSeconds: cr.Spec.RedisLeader.TerminationGracePeriodSeconds,
		NodeSelector:                  cr.Spec.RedisLeader.NodeSelector,
		Tolerations:                   cr.Spec.RedisLeader.Tolerations,
		ReadinessProbe:                cr.Spec.RedisLeader.ReadinessProbe,
		LivenessProbe:                 cr.Spec.RedisLeader.LivenessProbe,
	}
	if cr.Spec.RedisLeader.RedisConfig != nil {
		prop.ExternalConfig = cr.Spec.RedisLeader.RedisConfig.AdditionalRedisConfig
	}
	return prop.CreateRedisClusterSetup(cr)
}

// CreateRedisFollower will create a follower redis setup
func CreateRedisFollower(cr *redisv1beta1.RedisCluster) error {
	prop := RedisClusterSTS{
		RedisStateFulType:             "follower",
		SecurityContext:               cr.Spec.RedisFollower.SecurityContext,
		Affinity:                      cr.Spec.RedisFollower.Affinity,
		TerminationGracePeriodSeconds: cr.Spec.RedisFollower.TerminationGracePeriodSeconds,
		NodeSelector:                  cr.Spec.RedisFollower.NodeSelector,
		Tolerations:                   cr.Spec.RedisFollower.Tolerations,
		ReadinessProbe:                cr.Spec.RedisFollower.ReadinessProbe,
		LivenessProbe:                 cr.Spec.RedisFollower.LivenessProbe,
	}
	if cr.Spec.RedisFollower.RedisConfig != nil {
		prop.ExternalConfig = cr.Spec.RedisFollower.RedisConfig.AdditionalRedisConfig
	}
	return prop.CreateRedisClusterSetup(cr)
}

// CreateRedisLeaderService method will create service for Redis Leader
func CreateRedisLeaderService(cr *redisv1beta1.RedisCluster) error {
	prop := RedisClusterService{
		RedisServiceRole: "leader",
	}
	return prop.CreateRedisClusterService(cr)
}

// CreateRedisFollowerService method will create service for Redis Follower
func CreateRedisFollowerService(cr *redisv1beta1.RedisCluster) error {
	prop := RedisClusterService{
		RedisServiceRole: "follower",
	}
	return prop.CreateRedisClusterService(cr)
}

func (service RedisClusterSTS) getReplicaCount(cr *redisv1beta1.RedisCluster) int32 {
	return cr.Spec.GetReplicaCounts(service.RedisStateFulType)
}

// CreateRedisClusterSetup will create Redis Setup for leader and follower
func (service RedisClusterSTS) CreateRedisClusterSetup(cr *redisv1beta1.RedisCluster) error {
	// statefulset的名字为：redis-leader
	stateFulName := cr.ObjectMeta.Name + "-" + service.RedisStateFulType
	logger := statefulSetLogger(cr.Namespace, stateFulName)
	// 获取标签，主要有app=redis-leader,redis_setup_type=cluster,role=leader
	labels := getRedisLabels(stateFulName, "cluster", service.RedisStateFulType, cr.ObjectMeta.Labels)
	// 获取注解
	annotations := generateStatefulSetsAnots(cr.ObjectMeta)
	// 构建ObjectMeta，主要是设置名字，名称空间，标签以及注解
	objectMetaInfo := generateObjectMetaInformation(stateFulName, cr.Namespace, labels, annotations)
	// 创建或者更新statefulset
	err := CreateOrUpdateStateFul(
		cr.Namespace,
		objectMetaInfo,
		// 构建创建statefulset需要的参数
		generateRedisClusterParams(cr, service.getReplicaCount(cr), service.ExternalConfig, service),
		// 把RedisCluster资源作为StatefulSet的Owner
		redisClusterAsOwner(cr),
		// 构建Init容器参数
		generateRedisClusterInitContainerParams(cr),
		// 构建容器参数
		generateRedisClusterContainerParams(cr, service.SecurityContext, service.ReadinessProbe, service.LivenessProbe),
		cr.Spec.Sidecars,
	)
	if err != nil {
		logger.Error(err, "Cannot create statefulset for Redis", "Setup.Type", service.RedisStateFulType)
		return err
	}
	return nil
}

// CreateRedisClusterService method will create service for Redis
func (service RedisClusterService) CreateRedisClusterService(cr *redisv1beta1.RedisCluster) error {
	serviceName := cr.ObjectMeta.Name + "-" + service.RedisServiceRole
	logger := serviceLogger(cr.Namespace, serviceName)
	// 生成标签
	labels := getRedisLabels(serviceName, "cluster", service.RedisServiceRole, cr.ObjectMeta.Labels)
	// 生成注解
	annotations := generateServiceAnots(cr.ObjectMeta, nil)
	if cr.Spec.RedisExporter != nil && cr.Spec.RedisExporter.Enabled {
		enableMetrics = true
	} else {
		enableMetrics = false
	}
	additionalServiceAnnotations := map[string]string{}
	if cr.Spec.KubernetesConfig.Service != nil {
		additionalServiceAnnotations = cr.Spec.KubernetesConfig.Service.ServiceAnnotations
	}
	objectMetaInfo := generateObjectMetaInformation(serviceName, cr.Namespace, labels, annotations)
	headlessObjectMetaInfo := generateObjectMetaInformation(serviceName+"-headless", cr.Namespace, labels, annotations)
	additionalObjectMetaInfo := generateObjectMetaInformation(serviceName+"-additional", cr.Namespace, labels, generateServiceAnots(cr.ObjectMeta, additionalServiceAnnotations))
	err := CreateOrUpdateService(cr.Namespace, headlessObjectMetaInfo, redisClusterAsOwner(cr), false, true, "ClusterIP")
	if err != nil {
		logger.Error(err, "Cannot create headless service for Redis", "Setup.Type", service.RedisServiceRole)
		return err
	}
	err = CreateOrUpdateService(cr.Namespace, objectMetaInfo, redisClusterAsOwner(cr), enableMetrics, false, "ClusterIP")
	if err != nil {
		logger.Error(err, "Cannot create service for Redis", "Setup.Type", service.RedisServiceRole)
		return err
	}
	additionalServiceType := "ClusterIP"
	if cr.Spec.KubernetesConfig.Service != nil {
		additionalServiceType = cr.Spec.KubernetesConfig.Service.ServiceType
	}
	err = CreateOrUpdateService(cr.Namespace, additionalObjectMetaInfo, redisClusterAsOwner(cr), false, false, additionalServiceType)
	if err != nil {
		logger.Error(err, "Cannot create additional service for Redis", "Setup.Type", service.RedisServiceRole)
		return err
	}
	return nil
}
