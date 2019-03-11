extern crate timely;
extern crate hdrhist;
extern crate timely_affinity;

use std::time::Instant;
use std::rc::Rc;
use std::cell::RefCell;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Feedback, ConnectLoop};
use timely::dataflow::operators::generic::operator::Operator;

fn main() {

    let iterations = std::env::args().nth(1).unwrap().parse::<usize>().unwrap_or(1_000_000);

    let hists = ::timely_affinity::execute::execute_from_args(std::env::args().skip(2), move |worker| {

        let _index = worker.index();

        let hist1 = Rc::new(RefCell::new((hdrhist::HDRHist::new(), Vec::with_capacity(iterations + 2))));
        let hist2 = hist1.clone();

        worker.dataflow(move |scope| {
            let (handle, stream) = scope.feedback::<usize>(1);
            let mut t0 = Instant::now();
            stream.unary_notify(
                Pipeline,
                "Barrier",
                vec![0],
                move |_, _, notificator| {
                    while let Some((cap, _count)) = notificator.next() {
                        let t1 = Instant::now();
                        let duration = t1.duration_since(t0);
                        let duration_nanos = duration.as_secs() * 1_000_000_000u64 + duration.subsec_nanos() as u64;
                        {
                            use std::ops::DerefMut;
                            let mut borrow = hist1.borrow_mut();
                            let &mut (ref mut hist_ref, ref mut seq_ref) = borrow.deref_mut();
                            hist_ref.add_value(duration_nanos);
                            seq_ref.push(duration_nanos);
                        }
                        t0 = t1;
                        let time = *cap.time() + 1;
                        if time < iterations {
                            notificator.notify_at(cap.delayed(&time));
                        }
                    }
                }
            ).connect_loop(handle);
        });

        while worker.step() {}

        let hist = ::std::rc::Rc::try_unwrap(hist2).unwrap_or_else(|_err| panic!("Non unique owner"));
        hist.into_inner()
    }).unwrap().join(); // asserts error-free execution;

    // sorry for the hackish code, it's 9pm

    println!("-------------");
    println!("Worker 0");
    println!("Summary:\n{}", hists[0].as_ref().unwrap().0.summary_string());
    for entry in hists[0].as_ref().unwrap().0.ccdf_upper_bound() {
        println!("{:?}", entry);
    }

    let seq_0 = hists[0].as_ref().unwrap().1.clone();
    let seq_1 = hists[1].as_ref().unwrap().1.clone();

    println!("-------------");
    println!("All workers");
    let mut hist_iter = hists.into_iter().map(|x| x.unwrap().0);
    let head_hist = hist_iter.next().unwrap();
    let hist = hist_iter.into_iter().fold(head_hist, |acc, x| acc.combined(x));
    println!("Summary:\n{}", hist.summary_string());
    for entry in hist.ccdf_upper_bound() {
        println!("{:?}", entry);
    }

    println!("-------------");
    println!("Worker 0 samples");
    seq_0.iter().take(100).for_each(|x| println!("{}", x));

    println!("-------------");
    println!("Worker 1 samples");
    seq_1.iter().take(100).for_each(|x| println!("{}", x));
}
