use execute::Execute;
use log::{debug, info};
use structopt::StructOpt;

#[tokio::main]
async fn main() {
    let opt = KafkaClientOpt::from_args();
    let opt_clone = opt.clone();
    std::env::set_var("RUST_LOG", opt.rust_log.clone());
    env_logger::init();

    debug!("start pre_initial with config: {:#?}", opt_clone);

    const FFMPEG_PATH: &str = "/path/to/ffmpeg";

    let mut first_command = Command::new(FFMPEG_PATH);

    first_command.arg("-version");

    if first_command.execute_check_exit_status_code(0).is_err() {
        eprintln!(
            "The path `{}` is not a correct FFmpeg executable binary file.",
            FFMPEG_PATH
        );
    }
}
