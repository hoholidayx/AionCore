use super::*;
use crate::backend::{BackendConnection, SessionConfig, SessionSpec};
use crate::event::SessionEvent;
use crate::testing::FakeAgentIo;
use aionui_process::{FileRegistryStore, ManagedProcess, ProcessError, RealSpawner, Spawner};
use futures_util::StreamExt;
use std::sync::Arc;

#[tokio::test]
async fn wrong_home_and_unsupported_delete_fail_closed() {
    let expected = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let expected = std::fs::canonicalize(expected.path()).unwrap();
    let initialization = |path: &Path| json!({"id": 1, "result": {"codexHome": path}});

    let input = format!("{}\n", initialization(other.path()));
    let fake = FakeAgentIo::new(input.into_bytes(), None);
    let err = delete_thread(&fake, "test", &expected).await.unwrap_err();
    assert_eq!(
        err.to_string(),
        history_error("Codex data directory does not match the verified thread").to_string()
    );

    let input = format!(
        "{}\n{}\n",
        initialization(&expected),
        json!({"id": 2, "error": {"code": -32601}})
    );
    let fake = FakeAgentIo::new(input.into_bytes(), None);
    let err = delete_thread(&fake, "test", &expected).await.unwrap_err();
    assert_eq!(
        err.to_string(),
        history_error("This Codex version does not support permanent thread deletion").to_string()
    );
}

struct IsolatedSpawner {
    inner: RealSpawner<FileRegistryStore>,
    home: PathBuf,
}

#[async_trait::async_trait]
impl Spawner for IsolatedSpawner {
    async fn spawn(
        &self,
        spec: CommandSpec,
        _: &[(String, String)],
        tag: &str,
    ) -> Result<Arc<ManagedProcess>, ProcessError> {
        self.inner
            .spawn(
                spec,
                &[("CODEX_HOME".to_owned(), self.home.to_string_lossy().into_owned())],
                tag,
            )
            .await
    }
}

#[tokio::test]
#[ignore = "requires an installed Codex app-server; set AION_TEST_CODEX_BIN"]
async fn real_codex_records_fresh_thread_and_deletes_persisted_history() {
    let program = PathBuf::from(std::env::var_os("AION_TEST_CODEX_BIN").expect("AION_TEST_CODEX_BIN"));
    let temp = tempfile::tempdir().unwrap();
    let home = std::fs::canonicalize(temp.path()).unwrap();
    let spawner = Arc::new(IsolatedSpawner {
        inner: RealSpawner::new(Arc::new(FileRegistryStore::new(&home)), uuid::Uuid::new_v4(), "test"),
        home: home.clone(),
    });
    let connection = CodexConnection::new(spawner.clone()).with_history_provenance(true);
    let backend = connection
        .open_session(
            SessionSpec::Fresh {
                session_id: "test-fresh".into(),
            },
            SessionConfig {
                cwd: Some(home.to_string_lossy().into_owned()),
                cli_program: Some(program.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut events = backend.events();
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(event) = events.next().await {
            if let SessionEvent::CodexThreadCreated { thread_id, codex_home } = event.event {
                assert!(uuid::Uuid::parse_str(&thread_id).is_ok());
                assert_eq!(Path::new(&codex_home), home);
                return;
            }
        }
        panic!("missing verified fresh thread event");
    })
    .await
    .unwrap();
    backend.terminate().await;

    let io = ManagedProcessIo::new(
        spawner
            .spawn(
                CommandSpec {
                    command: program.clone(),
                    args: codex_app_server_args(&[]),
                    env: Vec::new(),
                    cwd: Some(home.to_string_lossy().into_owned()),
                },
                &[],
                "aionui-codex-history-test",
            )
            .await
            .unwrap(),
    );
    let (mut stdin, stdout) = io.take_stdio().await.unwrap();
    let mut lines = BufReader::new(stdout).lines();
    write_frame(&mut stdin, &initialize_params().into_frame(1, "initialize"))
        .await
        .unwrap();
    assert!(read_rpc_frame(&mut lines, 1).await.unwrap().get("result").is_some());
    write_frame(&mut stdin, &json!({"method": "initialized"}))
        .await
        .unwrap();
    write_frame(
        &mut stdin,
        &json!({"id": 2, "method": "thread/start", "params": {"ephemeral": false}}),
    )
    .await
    .unwrap();
    let started = read_rpc_frame(&mut lines, 2).await.unwrap();
    let thread_id = started.pointer("/result/thread/id").and_then(Value::as_str).unwrap();
    let path = Path::new(started.pointer("/result/thread/path").and_then(Value::as_str).unwrap());
    assert!(path.starts_with(&home));
    write_frame(&mut stdin, &json!({"id": 3, "method": "thread/inject_items", "params": {
        "threadId": thread_id, "items": [{"type":"message", "role":"user", "content":[{"type":"input_text", "text":"isolated test"}]}]
    }})).await.unwrap();
    assert!(read_rpc_frame(&mut lines, 3).await.unwrap().get("result").is_some());
    assert!(path.is_file());
    io.terminate().await;

    let other = tempfile::tempdir().unwrap();
    assert!(
        connection
            .delete_history(program.clone(), thread_id, other.path())
            .await
            .is_err()
    );
    assert!(path.is_file());
    connection
        .delete_history(program.clone(), thread_id, &home)
        .await
        .unwrap();
    assert!(!path.exists());
    connection.delete_history(program, thread_id, &home).await.unwrap();
}
