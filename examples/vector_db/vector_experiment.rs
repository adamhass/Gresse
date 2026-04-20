use gresse::{db::composite_view::CompositeView, db::ztable::ZTable, prelude::Pid};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_distr::{Distribution, Exp, StandardNormal};
use serde::{Deserialize, Serialize};
use statrs::distribution::{ChiSquared, ContinuousCDF};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::{fs::OpenOptions, time::sleep};

use crate::helpers::*;
use crate::vector_db::VectorDb;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExperimentConfig {
    pub db_dir_path: PathBuf,
    pub server_list_file_path: PathBuf,
    pub result_dir_path: PathBuf,
    pub config_path: PathBuf,
    // Experiment variables
    pub runtime: u32,            // runtime in seconds
    pub clients: u64,            // number of clients
    pub servers: u32,            // number of servers
    pub sync_interval: Duration, // synchronization interval
    pub dimensions: u32,         // vector dimensionality
    pub max_distance: Float,     // distance cut-off for saving in view
    pub init_s: u64,             // initial size of first Z-table
    pub init_u: u64,             // initial size of second Z-table
    pub k: usize,                // query argument (number of nearest vectors)
    pub eps: u32,                // events per second produced by the clients
    pub percent_reads: f64,      // query/mutate ratio for client events
    pub percent_inserts: f64,    // insert/remove ratio for client mutation events
    pub percent_s: f64,          // first/second table event ratio
}

impl ExperimentConfig {
    pub fn get_pids(&self) -> Vec<Pid> {
        let mut pids = Vec::new();
        for i in 0..self.servers {
            pids.push(i as Pid);
        }
        pids
    }

    pub fn prebuild_db(&self) -> VectorDb {
        let max_distance = self.max_distance;
        let db = match (
            ZTable::from_file(self.db_dir_path.clone().join("s.json")),
            ZTable::from_file(self.db_dir_path.clone().join("u.json")),
            CompositeView::from_file(self.db_dir_path.join("v.json")),
        ) {
            (Ok(s), Ok(u), Ok(v)) => {
                let s: ZTable<SKey, Vector> = s;
                let u: ZTable<SKey, Vector> = u;
                let v: CompositeView<SKey, Float> = v;
                print!("Prebuilt database read from {:?}", self.db_dir_path);
                VectorDb {
                    s,
                    u,
                    v,
                    max_distance,
                }
            }
            _ => {
                print!(
                    "No prebuild database under {:?}, building new",
                    self.db_dir_path
                );
                let mut db = VectorDb::new(max_distance);
                let seed = [255; 32];
                let mut rng = ChaCha8Rng::from_seed(seed);

                for i in 0..self.init_s {
                    db.insert_into_s(i, random_vector(&mut rng, self.dimensions));
                }
                for i in 0..self.init_u {
                    db.insert_into_u(i, random_vector(&mut rng, self.dimensions));
                }
                _ = db.save_db(self.db_dir_path.clone()).inspect_err(|_| {
                    eprintln!("Database could not be saved to file");
                });
                db
            }
        };
        assert_eq!(db.s.len(), self.init_s as usize);
        assert_eq!(db.u.len(), self.init_u as usize);
        assert!(db.v.len() > self.init_s as usize);
        db
    }

    /// Clears the server list
    pub async fn truncate_server_list(&self) {
        let _ = OpenOptions::new()
            .create(true) // Create the file if it doesn't exist
            .write(true) // Allow writing to the file
            .truncate(true)
            .open(&self.server_list_file_path)
            .await
            .expect("failed to truncate server list file");
    }
}

pub struct WorkloadGenerator {
    pub id: Key,
    pub s_index: Key,
    pub s_remove_index: Key,
    pub u_index: Key,
    pub u_remove_index: Key,
    pub clients: Key,
    pub rng: ChaCha8Rng,
    pub eps: f64,
    pub percent_reads: f64,
    // Used for new elements
    pub dimensions: u32,
    pub percent_s: f64,
    pub percent_inserts: f64,
    // Used for queries
    pub k: usize,
    pub init_s: Key,
    pub init_u: Key,
    pub last_event_time: Instant,
    pub sleep_distribution: Exp<f64>,
}

impl WorkloadGenerator {
    pub fn from_config(cfg: &ExperimentConfig) -> Vec<WorkloadGenerator> {
        let mut r = Vec::new();
        let mut seed = [0; 32];
        for i in 0..cfg.clients {
            seed[0] = i as u8;
            let eps = cfg.eps as f64 / cfg.clients as f64;
            r.push(WorkloadGenerator {
                id: i,
                s_index: cfg.init_s
                    + 1
                    + (i + cfg.clients - (cfg.init_s + 1) % cfg.clients) % cfg.clients,
                u_index: cfg.init_u
                    + 1
                    + (i + cfg.clients - (cfg.init_u + 1) % cfg.clients) % cfg.clients,
                clients: cfg.clients,
                rng: ChaCha8Rng::from_seed(seed),
                dimensions: cfg.dimensions,
                k: cfg.k,
                eps,
                percent_reads: cfg.percent_reads,
                percent_s: cfg.percent_s,
                percent_inserts: cfg.percent_inserts,
                init_s: cfg.init_s,
                init_u: cfg.init_u,
                last_event_time: Instant::now(),
                sleep_distribution: Exp::new(eps).expect("failed to create distr."),
                s_remove_index: i,
                u_remove_index: i,
            });
        }
        r
    }

    pub fn set_start(&mut self) {
        self.last_event_time = Instant::now()
    }

    /// Returns a newly generated request every 1/eps seconds
    pub async fn get_next(&mut self) -> DbRequest {
        // Delay event generation
        self.delay().await;
        self.get_next_event()
    }

    pub fn get_next_event(&mut self) -> DbRequest {
        if self.rng.random::<f64>() < self.percent_reads {
            // DbRequest::query(self.rng.random_range(0..self.init_s), self.k)
            DbRequest::query(
                self.s_remove_index
                    + self
                        .rng
                        .random_range(0..((self.s_index - self.s_remove_index + 1) / self.clients))
                        * self.clients,
                self.k,
            )
        } else {
            let vector = random_vector(&mut self.rng, self.dimensions);
            if self.rng.random::<f64>() < self.percent_s {
                if self.rng.random::<f64>() < self.percent_inserts {
                    self.s_index += self.clients;
                    DbRequest::vector(EitherKey::S(self.s_index - self.clients), vector)
                } else {
                    self.s_remove_index += self.clients;
                    DbRequest::remove(EitherKey::S(self.s_remove_index - self.clients))
                }
            } else if self.rng.random::<f64>() < self.percent_inserts {
                self.u_index += self.clients;
                DbRequest::vector(EitherKey::U(self.u_index - self.clients), vector)
            } else {
                self.u_remove_index += self.clients;
                DbRequest::remove(EitherKey::U(self.u_remove_index - self.clients))
            }
        }
    }

    async fn delay(&mut self) {
        // Calculate ideal time between events (in seconds)
        let delay_time = self.sleep_distribution.sample(&mut self.rng);
        let delay_duration = Duration::from_secs_f64(delay_time);

        // Calculate how much time has passed since the last event
        let elapsed_time = self.last_event_time.elapsed();

        // If the elapsed time is less than the ideal time, sleep for the remaining time
        if elapsed_time < delay_duration {
            let sleep_time = delay_duration - elapsed_time;
            sleep(sleep_time).await;
        }
        // Update the last event time to catch up if we're behind schedule
        self.last_event_time += delay_duration;
    }
}

pub fn random_vector(rng: &mut ChaCha8Rng, d: Dimensions) -> Vector {
    (0..d).map(|_| Float(StandardNormal.sample(rng))).collect()
}

pub fn get_max_distance(selectivity: f64, dimensions: u32) -> Result<Float, &'static str> {
    if !(0.0..=1.0).contains(&selectivity) {
        return Err("Selectivity must be between 0.0 and 1.0");
    }
    let chi_distr =
        ChiSquared::new(dimensions as f64).map_err(|_| "Invalid number of dimensions")?;

    let max_distance = if selectivity == 0.0 {
        -1.0
    } else if selectivity < 1.0 {
        (2.0 * chi_distr.inverse_cdf(selectivity)).sqrt() as f32
    } else {
        f32::MAX
    };
    Ok(Float(max_distance))
}

#[cfg(test)]
mod tests {
    use itertools::iproduct;

    use crate::helpers;

    use super::*;

    // Creates a generator, runs it for eps*runtime events, and returns the runtime in seconds
    async fn test_workload_generator_events_per_second(eps: f64, runtime: f64) -> f64 {
        // Create a WorkloadGenerator with a fixed seed
        let mut generator = WorkloadGenerator {
            id: 0,
            s_index: 100,
            s_remove_index: 0,
            u_index: 50,
            u_remove_index: 0,
            clients: 1,
            rng: ChaCha8Rng::from_seed([42; 32]),
            eps,
            percent_reads: 0.5,
            dimensions: 4,
            percent_s: 0.7,
            percent_inserts: 0.5,
            k: 5,
            init_s: 100,
            init_u: 50,
            last_event_time: Instant::now(),
            sleep_distribution: Exp::new(eps).expect("failed to create distr."),
        };
        let start_time = Instant::now();
        // Generate some events,
        for _ in 0..(eps * runtime) as usize {
            let _ = generator.get_next().await;
        }
        let time = start_time.elapsed();
        time.as_secs() as f64
    }

    #[tokio::test]
    async fn test_high_eps() {
        let eps = 100.0;
        let runtime = 10.0;
        let time = test_workload_generator_events_per_second(eps, runtime).await;
        // Allow 50% margin of error for the randomness
        assert!(time > 5.0);
        assert!(time < 15.0);
    }

    #[tokio::test]
    async fn test_low_eps() {
        let eps = 0.1;
        let runtime = 100.0;
        let time = test_workload_generator_events_per_second(eps, runtime).await;
        // Allow 50% margin of error for the randomness
        assert!(time > 50.0);
        assert!(time < 150.0);
    }

    #[test]
    fn test_vector_generation_selectivity() {
        let seed = [255; 32];
        let mut rng = ChaCha8Rng::from_seed(seed);

        let selectivities = [0.0, 0.05, 0.2, 0.5, 0.8, 1.0];
        let dimensions = [1, 4, 32, 128];
        for (sel, dims) in iproduct!(selectivities.into_iter(), dimensions.into_iter()) {
            let max_dist = get_max_distance(sel, dims).unwrap();
            let mut count = 0.0;
            for _ in 0..10000 {
                let v1 = random_vector(&mut rng, dims);
                let v2 = random_vector(&mut rng, dims);
                if helpers::l2_norm(&v1, &v2) <= max_dist {
                    count += 1.0;
                }
            }
            assert!(sel - 0.05 < count / 10000.0 && count / 10000.0 < sel + 0.05)
        }
    }
}
