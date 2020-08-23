use smalltask::ParallelSlice;
use smalltask::TaskPool;

// This example demonstrates how to parallel iterate a slice of data in chunks

fn main() {
    let messages: Vec<_> = (0..100).collect();

    let task_pool = TaskPool::builder().build();

    let outputs = messages.par_chunk_map(&task_pool, 10, |values| {
        let mut sum = 0;
        for value in values {
            println!(
                "Processing value {} on thread {:?}",
                value,
                std::thread::current().id()
            );
            sum += value;
        }

        sum
    });

    println!("sums: {:?}", outputs);
}
