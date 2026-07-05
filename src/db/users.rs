use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct User {
    pub id: i64,
    pub handle: String,
    pub role: String,
    pub created_at: DateTime<Utc>,
    pub disabled_at: Option<DateTime<Utc>>,
}

impl User {
    pub fn is_admin(&self) -> bool {
        self.role == "admin"
    }

    pub fn is_active(&self) -> bool {
        self.disabled_at.is_none()
    }
}

pub async fn create(pool: &PgPool, handle: &str, role: &str) -> Result<User> {
    let user = sqlx::query_as!(
        User,
        r#"insert into users (handle, role) values ($1, $2)
           returning id, handle, role, created_at, disabled_at"#,
        handle,
        role,
    )
    .fetch_one(pool)
    .await?;
    Ok(user)
}

pub async fn get(pool: &PgPool, id: i64) -> Result<Option<User>> {
    let user = sqlx::query_as!(
        User,
        "select id, handle, role, created_at, disabled_at from users where id = $1",
        id,
    )
    .fetch_optional(pool)
    .await?;
    Ok(user)
}

pub async fn get_by_handle(pool: &PgPool, handle: &str) -> Result<Option<User>> {
    let user = sqlx::query_as!(
        User,
        "select id, handle, role, created_at, disabled_at from users where handle = $1",
        handle,
    )
    .fetch_optional(pool)
    .await?;
    Ok(user)
}

pub async fn list(pool: &PgPool) -> Result<Vec<User>> {
    let users = sqlx::query_as!(
        User,
        "select id, handle, role, created_at, disabled_at from users order by id",
    )
    .fetch_all(pool)
    .await?;
    Ok(users)
}

pub async fn set_disabled(pool: &PgPool, id: i64, disabled: bool) -> Result<()> {
    sqlx::query!(
        r#"update users
           set disabled_at = case when $2 then now() else null end
           where id = $1"#,
        id,
        disabled,
    )
    .execute(pool)
    .await?;
    Ok(())
}
