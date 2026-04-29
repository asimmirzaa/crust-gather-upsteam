use k8s_openapi::api::core::v1::{Container, EphemeralContainer, Pod};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PodContainerKind {
    App,
    Init,
    Ephemeral,
}

impl std::fmt::Display for PodContainerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::App => write!(f, "app"),
            Self::Init => write!(f, "init"),
            Self::Ephemeral => write!(f, "ephemeral"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PodContainerRef {
    pub name: String,
    pub image: Option<String>,
    pub kind: PodContainerKind,
}

pub fn pod_container_refs(pod: &Pod) -> Vec<PodContainerRef> {
    let Some(spec) = pod.spec.as_ref() else {
        return vec![];
    };

    let mut containers = spec
        .containers
        .iter()
        .map(|container| to_container_ref(container, PodContainerKind::App))
        .collect::<Vec<_>>();

    containers.extend(
        spec.init_containers
            .clone()
            .unwrap_or_default()
            .iter()
            .map(|container| to_container_ref(container, PodContainerKind::Init)),
    );
    containers.extend(
        spec.ephemeral_containers
            .clone()
            .unwrap_or_default()
            .iter()
            .map(|container| to_ephemeral_container_ref(container, PodContainerKind::Ephemeral)),
    );

    containers
}

fn to_container_ref(container: &Container, kind: PodContainerKind) -> PodContainerRef {
    PodContainerRef {
        name: container.name.clone(),
        image: container.image.clone(),
        kind,
    }
}

fn to_ephemeral_container_ref(
    container: &EphemeralContainer,
    kind: PodContainerKind,
) -> PodContainerRef {
    PodContainerRef {
        name: container.name.clone(),
        image: container.image.clone(),
        kind,
    }
}

#[cfg(test)]
mod tests {
    use k8s_openapi::api::core::v1::{EphemeralContainer, Pod, PodSpec};

    use super::{PodContainerKind, pod_container_refs};

    #[test]
    fn pod_container_refs_include_all_container_types() {
        let pod = Pod {
            spec: Some(PodSpec {
                containers: vec![k8s_openapi::api::core::v1::Container {
                    name: "app".to_string(),
                    image: Some("app:v1".to_string()),
                    ..Default::default()
                }],
                init_containers: Some(vec![k8s_openapi::api::core::v1::Container {
                    name: "init".to_string(),
                    image: Some("init:v1".to_string()),
                    ..Default::default()
                }]),
                ephemeral_containers: Some(vec![EphemeralContainer {
                    name: "debug".to_string(),
                    image: Some("debug:v1".to_string()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let refs = pod_container_refs(&pod);
        assert_eq!(refs.len(), 3);
        assert_eq!(refs[0].kind, PodContainerKind::App);
        assert_eq!(refs[1].kind, PodContainerKind::Init);
        assert_eq!(refs[2].kind, PodContainerKind::Ephemeral);
    }
}
