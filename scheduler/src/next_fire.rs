use chrono::{DateTime, Utc};
use cron::Schedule as CronSchedule;
use std::str::FromStr;

use crate::store::Schedule;

/// Compute the next fire time for a schedule, returning an ISO 8601 string.
pub fn compute_next_fire(schedule: &Schedule, now: DateTime<Utc>) -> Option<String> {
    match schedule.schedule_type.as_str() {
        "cron" => compute_cron_next(schedule, now),
        "interval" => compute_interval_next(schedule, now),
        "once" => {
            if schedule.status == "completed" {
                None
            } else {
                schedule.run_at.clone()
            }
        }
        _ => None,
    }
}

/// Normalize a cron expression to the 7-field format expected by the `cron` crate
/// (sec min hour dom month dow [year]).
///
/// Users typically write 5-field cron (min hour dom month dow). We detect this
/// by field count and prepend "0 " (seconds = 0) to make it 7-field.
fn normalize_cron_expr(expr: &str) -> String {
    let field_count = expr.split_whitespace().count();
    match field_count {
        5 => format!("0 {expr} *"),   // min hour dom month dow → 0 min hour dom month dow *
        6 => format!("0 {expr}"),     // sec min hour dom month dow → add seconds=0 prefix
        _ => expr.to_string(),        // 7-field or shorthand — pass through
    }
}

fn compute_cron_next(schedule: &Schedule, now: DateTime<Utc>) -> Option<String> {
    let raw_expr = schedule.cron_expression.as_deref()?;
    let expr = normalize_cron_expr(raw_expr);

    let cron = CronSchedule::from_str(&expr).ok()?;

    // Parse timezone
    let tz: chrono_tz::Tz = schedule.timezone.parse().unwrap_or(chrono_tz::UTC);

    // Convert `now` to the schedule's timezone and iterate
    let now_in_tz = now.with_timezone(&tz);
    let next_in_tz = cron.after(&now_in_tz).next()?;
    let next_utc = next_in_tz.with_timezone(&Utc);

    Some(next_utc.to_rfc3339())
}

fn compute_interval_next(schedule: &Schedule, now: DateTime<Utc>) -> Option<String> {
    let interval_secs = schedule.interval_seconds?;
    if interval_secs < 10 {
        return None;
    }

    let base = match &schedule.last_fired {
        Some(ts) => ts.parse::<DateTime<Utc>>().ok()?,
        None => now,
    };

    let next = base + chrono::Duration::seconds(interval_secs as i64);
    // If next is already in the past (e.g. woke from sleep), schedule from now
    if next <= now {
        Some((now + chrono::Duration::seconds(interval_secs as i64)).to_rfc3339())
    } else {
        Some(next.to_rfc3339())
    }
}
