extern crate timely;
extern crate streaming_harness_hdrhist;
extern crate timely_affinity;

use std::time::Instant;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Feedback, ConnectLoop};
use timely::dataflow::operators::generic::operator::Operator;

fn main() {

    let iterations = std::env::args().nth(1).unwrap().parse::<usize>().unwrap_or(1_000_000);

    ::timely_affinity::execute::execute_from_args(std::env::args().skip(2), move |worker| {

        let index = worker.index();

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
    }).unwrap(); // asserts error-free execution;
}

