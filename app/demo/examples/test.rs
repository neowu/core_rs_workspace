use std::collections::HashMap;
use std::time::Duration;

use tokio::task::Id;
use tokio::task::JoinSet;
use tokio::time::timeout;

#[tokio::main]
async fn main() {
    let mut set: JoinSet<usize> = JoinSet::new();
    let mut names: HashMap<Id, String> = HashMap::new();

    for i in 0..10 {
        let abort = set.spawn(async move {
            tokio::time::sleep(Duration::from_secs(i as u64)).await;
            i
        });
        names.insert(abort.id(), format!("task-{i}"));
    }

    // Drain finished tasks until timeout
    let _ = timeout(Duration::from_secs(3), async {
        while let Some(res) = set.join_next_with_id().await {
            match res {
                Ok((id, _value)) => {
                    names.remove(&id);
                } // finished cleanly
                Err(e) => {
                    names.remove(&e.id());
                } // panicked/cancelled
            }
        }
    })
    .await;

    // Whatever's still in `names` is unfinished
    println!("Unfinished: {:?}", names.values().collect::<Vec<_>>());

    // Abort the stragglers
    set.shutdown().await;
}
