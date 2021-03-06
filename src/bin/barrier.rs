extern crate timely;
extern crate hdrhist;
extern crate getopts;
extern crate timely_affinity;

use std::time::Instant;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Feedback, ConnectLoop};
use timely::dataflow::operators::generic::operator::Operator;

fn main() {

    let iterations = std::env::args().nth(1).unwrap().parse::<usize>().unwrap_or(1_000_000);
    // currently need timely's full option set to parse args
    let mut opts = getopts::Options::new();
    opts.optopt("w", "workers", "", "");
    opts.optopt("n", "processes", "", "");
    opts.optopt("p", "process_id", "", "");
    opts.optopt("h", "hostfile", "", "");
    opts.optflag("s", "serialize", "use the zero_copy serialising allocator");
    if let Ok(matches) = opts.parse(std::env::args().skip(2)) {
        let workers: usize = matches.opt_str("w").map(|x| x.parse().unwrap_or(1)).unwrap_or(1);
        let serialize: bool = matches.opt_present("s");
        macro_rules! worker_closure { () => (move |worker| {
            let index = worker.index();

            worker.dataflow(move |scope| {
                let (handle, stream) = scope.feedback::<usize>(1);
                let mut hist = hdrhist::HDRHist::new();
                let mut t0 = Instant::now();
                stream.unary_notify(
                    Pipeline,
                    "Barrier",
                    vec![0],
                    move |_, _, notificator| {
                        while let Some((cap, _count)) = notificator.next() {
                            let t1 = Instant::now();
                            let duration = t1.duration_since(t0);
                            hist.add_value(duration.as_secs() * 1_000_000_000u64 + duration.subsec_nanos() as u64);
                            t0 = t1;
                            let time = *cap.time() + 1;
                            if time < iterations {
                                notificator.notify_at(cap.delayed(&time));
                            } else {
                                if index == 0 {
                                    println!("-------------\nSummary:\n{}", hist.summary_string());
                                    println!("-------------\nCDF:");
                                    for entry in hist.ccdf_upper_bound() {
                                        println!("{:?}", entry);
                                    }
                                }
                            }
                        }
                    }
                )
                    .connect_loop(handle);
            });
        });
        }
        if !serialize {
            ::timely_affinity::execute::execute_from_args(std::env::args().skip(2), worker_closure!()).unwrap();
        } else {
            eprintln!("NOTE: thread allocators zerocopy, this ignores -p and -n");
            let allocators =
                ::timely::communication::allocator::zero_copy::allocator_process::ProcessBuilder::new_vector(workers);
            ::timely_affinity::execute::execute_from(
                std::env::args().skip(2).filter(|arg| arg != "-s"), allocators, Box::new(()), worker_closure!()).unwrap();
        }
    } else {
        println!("error parsing arguments");
        println!("usage:\tbarrier <iterations> (worker|process) [timely options]");
    }
}

