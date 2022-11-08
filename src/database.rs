use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool as Pool_, PooledConnection};
use diesel_migrations::{
  embed_migrations, EmbeddedMigrations, HarnessWithOutput, MigrationHarness,
};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::settings::get_settings;

pub type Pool = Pool_<ConnectionManager<PgConnection>>;
pub type Conn = PooledConnection<ConnectionManager<PgConnection>>;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

// WARNING! This function SHOULD called only once.
//
// The Rust compiler is allowed to assume that the value a shared
// reference points to will not change while that reference lives.
// POOL is unsafely mutated only once on the setup function.
static mut POOL: Option<Pool> = None;
static IS_INITIALIZED: AtomicBool = AtomicBool::new(false);

pub fn setup() -> Result<(), String> {
  let exchange = IS_INITIALIZED.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed);
  let can_setup = exchange == Ok(false);

  if !can_setup {
    panic!("Database already initialized");
  }

  let settings = get_settings();
  let url = settings.database.url.as_str();

  let manager = ConnectionManager::<PgConnection>::new(url);

  let pool = Pool::builder()
    .test_on_check_out(true)
    .build(manager)
    // TODO: return the original error instead?
    .map_err(|err| format!("{:?}", err))?;

  let con = &mut pool
    .clone()
    .get()
    // TODO: return the original error instead?
    .map_err(|err| format!("{:?}", err))?;

  HarnessWithOutput::write_to_stdout(con)
    .run_pending_migrations(MIGRATIONS)
    // TODO: return the original error instead?
    .map_err(|err| format!("{:?}", err))?;

  unsafe {
    POOL = Some(pool);
  };

  Ok(())
}

pub fn get_connection() -> &'static mut Conn {
  unsafe {
    &mut POOL
      .as_ref()
      .expect("Database pool not initialized")
      .get()
      .expect("Failed to get connection from pool")
  }
}
