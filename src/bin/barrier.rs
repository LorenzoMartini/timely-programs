extern crate timely;
extern crate streaming_harness_hdrhist;
extern crate core_affinity;
extern crate getopts;

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
    if let Ok(_matches) = opts.parse(std::env::args().skip(2)) {
        macro_rules! worker_closure { () => (move |worker| {
            let index = worker.index();
            // Pin core
            let core_ids = core_affinity::get_core_ids().unwrap();
            core_affinity::set_for_current(core_ids[index % core_ids.len()]);

            worker.dataflow(move |scope| {
                let (handle, stream) = scope.feedback::<usize>(1);
                let mut hist = streaming_harness_hdrhist::HDRHist::new();
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
                                    for entry in hist.ccdf() {
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
        
        ::timely::execute_from_args(std::env::args().skip(2), worker_closure!()).unwrap();
    } else {
        println!("error parsing arguments");
        println!("usage:\tbarrier <iterations> (worker|process) [timely options]");
    }
}

