use std::env;
use std::process::exit;
use std::thread::available_parallelism;
use std::time::Duration;

const USAGE: &str = "\
usage: http_test_client [options]

  --url         <url>   server base url, default http://localhost:8080
  --scenario    <name>  get | post | api_get | api_post, default get
                        get/post hit the plain controllers, api_* hit the #[api] generated routes
  --concurrency <n>     in flight requests, all multiplexed on one h2c connection, default 64
  --duration    <secs>  measured phase, default 30
  --warmup      <secs>  discarded phase before the measured one, default 5
  --values      <n>     number of values in the post body, default 10
  --threads     <n>     client runtime worker threads, default available parallelism
  --record              also print the machine readable result line run.sh records
";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    Get,
    Post,
    ApiGet,
    ApiPost,
}

impl Scenario {
    pub const fn as_str(self) -> &'static str {
        match self {
            Scenario::Get => "get",
            Scenario::Post => "post",
            Scenario::ApiGet => "api_get",
            Scenario::ApiPost => "api_post",
        }
    }

    /// Path on the server, the api variants serve the same payloads through the generated route.
    pub const fn path(self) -> &'static str {
        match self {
            Scenario::Get => "/benchmark/get",
            Scenario::Post => "/benchmark/post",
            Scenario::ApiGet => "/benchmark/api/get",
            Scenario::ApiPost => "/benchmark/api/post",
        }
    }

    pub const fn is_post(self) -> bool {
        matches!(self, Scenario::Post | Scenario::ApiPost)
    }
}

#[derive(Debug)]
pub struct Args {
    pub url: String,
    pub scenario: Scenario,
    pub concurrency: usize,
    pub duration: Duration,
    pub warmup: Duration,
    pub values: usize,
    pub threads: usize,
    pub record: bool,
}

impl Default for Args {
    fn default() -> Self {
        Args {
            url: "http://localhost:8080".to_owned(),
            scenario: Scenario::Get,
            concurrency: 64,
            duration: Duration::from_secs(30),
            warmup: Duration::from_secs(5),
            values: 10,
            threads: available_parallelism().map_or(4, |value| value.get()),
            record: false,
        }
    }
}

impl Args {
    /// Fails the process on anything unrecognized, a mistyped flag must not silently run a
    /// different benchmark than the one intended.
    pub fn parse() -> Self {
        let mut args = Args::default();
        let mut iter = env::args().skip(1);

        while let Some(key) = iter.next() {
            if key == "--help" || key == "-h" {
                print!("{USAGE}");
                exit(0);
            }
            if key == "--record" {
                args.record = true;
                continue;
            }
            let value = iter.next().unwrap_or_else(|| fail(&format!("missing value, option={key}")));
            match key.as_str() {
                "--url" => args.url = value.trim_end_matches('/').to_owned(),
                "--scenario" => {
                    args.scenario = match value.as_str() {
                        "get" => Scenario::Get,
                        "post" => Scenario::Post,
                        "api_get" => Scenario::ApiGet,
                        "api_post" => Scenario::ApiPost,
                        _ => fail(&format!("unknown scenario, value={value}")),
                    };
                }
                "--concurrency" => args.concurrency = number(&key, &value),
                "--duration" => args.duration = Duration::from_secs(number(&key, &value) as u64),
                "--warmup" => args.warmup = Duration::from_secs(number(&key, &value) as u64),
                "--values" => args.values = number(&key, &value),
                "--threads" => args.threads = number(&key, &value),
                _ => fail(&format!("unknown option, option={key}")),
            }
        }

        if args.concurrency == 0 {
            fail::<()>("concurrency must be greater than 0");
        }

        args
    }
}

fn number(key: &str, value: &str) -> usize {
    value.parse().unwrap_or_else(|_| fail(&format!("invalid number, option={key}, value={value}")))
}

fn fail<T>(message: &str) -> T {
    eprintln!("error: {message}\n\n{USAGE}");
    exit(1);
}
