* refactor shutdown with CancellationToken
* action log records message_handler, job_handler, task_handler? only by macro with file!()/line!()/column!()?
* make state with Box::leak()?
* log_collector supports collect cookies?
* scheduler: impl web trigger?

# Thoughts on AI era
1. require more streamlined build server? no ui, drive by api / slack?, which can dynamically create new kube deployment?
  or just static infra, incubate prototype in like prototype-service, then move to designated service
2. how to handle verification? integration test on local with db/kafka container? more automated flack test service on DEV?
