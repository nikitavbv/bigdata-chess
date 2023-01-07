use {
    std::{collections::BTreeMap, ops::Deref},
    tracing::info,
    sqlx::postgres::PgPoolOptions,
    tokio_postgres::{NoTls, Statement, Socket, tls::{MakeTlsConnect, TlsConnect}},
    bb8::{CustomizeConnection, Pool},
    bb8_postgres::PostgresConnectionManager,
    crate::{
        config::DatabaseConfig,
        entity::{ChessGameEntity, ChessGameMoveEntity},
    },
};

pub struct Database {
    pool: sqlx::postgres::PgPool,
    bb8_pool: bb8::Pool<CustomPostgresConnectionManager<NoTls>>,

    statement_insert_game_move: Statement,
}

impl Database {
    pub async fn new(config: &DatabaseConfig) -> Self {
        info!("connecting to database...");

        let pg_mgr = CustomPostgresConnectionManager::new(config.connection_string().unwrap().parse().unwrap(), NoTls);
        let bb8_pool = Pool::builder()
            .max_size(4)
            .connection_customizer(Box::new(Customizer))
            .build(pg_mgr)
            .await
            .unwrap();

        info!("preparing statements");
        let statement_insert_game_move = bb8_pool.get().await.unwrap().prepare("insert into chess_game_moves (id, game_id, from_file, from_rank, to_file, to_rank) values ($1, $2, $3, $4, $5, $6) on conflict do nothing").await.unwrap();

        info!("connected to database");
        Self {
            pool: PgPoolOptions::new()
                .max_connections(5)
                .connect(config.connection_string().unwrap())
                .await
                .unwrap(),
            bb8_pool,

            statement_insert_game_move, 
        }
    }

    pub async fn save_game(&self, game: ChessGameEntity) {
        sqlx::query("insert into chess_games (id, opening, white_player_elo) values ($1, $2, $3) on conflict do nothing")
            .bind(game.id())
            .bind(game.opening())
            .bind(game.white_player_elo() as i32)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    pub async fn save_game_move(&self, game_move: ChessGameMoveEntity) {
        let connection = self.bb8_pool.get().await.unwrap();

        connection.query(connection.custom_state.get(&QueryName::SaveGameMove).unwrap(), &[
            &game_move.id(),
            &game_move.game_id(),
            &game_move.from_file().map(|v| v as i32),
            &game_move.from_rank().map(|v| v as i32),
            &game_move.to_file().map(|v| v as i32),
            &game_move.to_rank().map(|v| v as i32)
        ]).await.unwrap();
    }
}

#[derive(Debug)]
struct Customizer;

#[async_trait::async_trait]
impl CustomizeConnection<CustomPostgresConnection, tokio_postgres::Error> for Customizer {
    async fn on_acquire(&self, conn: &mut CustomPostgresConnection) -> Result<(), tokio_postgres::Error> {
        conn.custom_state
            .insert(QueryName::SaveGameMove, conn.prepare("insert into chess_game_moves (id, game_id, from_file, from_rank, to_file, to_rank) values ($1, $2, $3, $4, $5, $6) on conflict do nothing").await?);
        Ok(())
    }
}

struct CustomPostgresConnectionManager<Tls> where Tls: MakeTlsConnect<Socket> {
    inner: PostgresConnectionManager<Tls>,
}

impl<Tls> CustomPostgresConnectionManager<Tls> where Tls: MakeTlsConnect<Socket> {
    pub fn new(config: tokio_postgres::Config, tls: Tls) -> Self {
        Self {
            inner: PostgresConnectionManager::new(config, tls),
        }
    }
}

#[async_trait::async_trait]
impl<Tls> bb8::ManageConnection for CustomPostgresConnectionManager<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type Connection = CustomPostgresConnection;
    type Error = tokio_postgres::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let conn = self.inner.connect().await?;
        Ok(CustomPostgresConnection::new(conn))
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.simple_query("").await.map(|_| ())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        self.inner.has_broken(&mut conn.inner)
    }
}

struct CustomPostgresConnection {
    inner: tokio_postgres::Client,
    custom_state: BTreeMap<QueryName, Statement>,
}

impl CustomPostgresConnection {
    fn new(inner: tokio_postgres::Client) -> Self {
        Self {
            inner,
            custom_state: Default::default(),
        }
    }
}

impl Deref for CustomPostgresConnection {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
enum QueryName {
    SaveGameMove,
}