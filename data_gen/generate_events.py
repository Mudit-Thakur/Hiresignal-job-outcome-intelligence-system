import csv
import os
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "raw_events.csv")

FUNNEL_STEPS = [
    "session_start",
    "signup",
    "profile_complete",
    "resume_upload",
    "job_search",
    "job_view",
    "job_apply",
    "interview_invite",
]

CONTINUE_PROB = {
    "session_start":    0.72,
    "signup":           0.80,
    "profile_complete": 0.75,
    "resume_upload":    0.85,
    "job_search":       0.78,
    "job_view":         0.55,
    "job_apply":        0.30,
}

# ─── Return probability profiles by user outcome ──────────────────────────────
# This is the KEY fix — behavior after week 0 depends on what happened to user
#
# got_invite=True  → user is excited, keeps coming back, low churn
# was_ghosted=True → user loses trust week by week, high churn
# neither          → neutral browser, moderate decay
#
# Format: (base_prob, decay_per_week, minimum_floor)
RETURN_PROFILES = {
    "got_invite":    (0.90, 0.04, 0.55),   # high retention, slow decay, stays engaged
    "ghosted_only":  (0.70, 0.18, 0.05),   # fast trust erosion, drops off quickly
    "neutral":       (0.75, 0.10, 0.20),   # moderate decay
}


def random_ts(base: datetime, min_minutes: int, max_minutes: int) -> datetime:
    return base + timedelta(minutes=random.randint(min_minutes, max_minutes))


def generate_return_sessions(
    user_id: str,
    first_event_date: datetime,
    device: str,
    city: str,
    source: str,
    num_weeks: int,
    got_invite: bool,
    was_ghosted: bool,
) -> list[dict]:
    """
    Simulate return visits across weeks 1-11.
    Return probability profile is determined by user outcome:
      - Got interview invite → stays engaged (high retention)
      - Ghosted only         → loses trust fast (high churn)
      - Neither              → moderate decay
    """
    events = []

    # Pick the right behavioral profile
    if got_invite:
        base_prob, decay, floor = RETURN_PROFILES["got_invite"]
    elif was_ghosted:
        base_prob, decay, floor = RETURN_PROFILES["ghosted_only"]
    else:
        base_prob, decay, floor = RETURN_PROFILES["neutral"]

    for week_num in range(1, num_weeks + 1):
        # Probability of returning this week
        return_prob = max(floor, base_prob - (week_num * decay))

        if random.random() > return_prob:
            # User didn't return this week — but might come back later
            # Ghosted users who miss a week are more likely to fully churn
            if was_ghosted and not got_invite:
                # Ghosted users who skip a week rarely come back
                if random.random() > 0.25:
                    break
            continue

        # Return visit: pick session start time during this week
        session_start = first_event_date + timedelta(
            days=(week_num * 7) + random.randint(0, 6),
            hours=random.randint(7, 22),
            minutes=random.randint(0, 59),
        )

        # Got-invite users do more per session — they're motivated
        if got_invite:
            session_event_count = random.randint(3, 6)
        elif was_ghosted:
            session_event_count = random.randint(1, 3)
        else:
            session_event_count = random.randint(2, 4)

        session_events = ["session_start"] + random.choices(
            ["job_search", "job_view", "job_apply", "irrelevant_job_shown"],
            k=session_event_count,
        )

        current_ts = session_start
        for evt in session_events:
            events.append({
                "event_id":   fake.uuid4(),
                "user_id":    user_id,
                "event_type": evt,
                "created_at": current_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "device":     device,
                "city":       city,
                "source":     source,
            })
            current_ts = random_ts(current_ts, 2, 45)

        # Ghosting can happen in return sessions too
        if "job_apply" in session_events:
            if got_invite:
                # Users with invites get ghosted less — recruiters responded before
                if random.random() < 0.20:
                    events.append({
                        "event_id":   fake.uuid4(),
                        "user_id":    user_id,
                        "event_type": "ghosted_by_recruiter",
                        "created_at": random_ts(current_ts, 60, 4320).strftime("%Y-%m-%d %H:%M:%S"),
                        "device":     device,
                        "city":       city,
                        "source":     source,
                    })
            elif was_ghosted:
                # Already ghosted once — keeps happening to them
                if random.random() < 0.70:
                    events.append({
                        "event_id":   fake.uuid4(),
                        "user_id":    user_id,
                        "event_type": "ghosted_by_recruiter",
                        "created_at": random_ts(current_ts, 60, 4320).strftime("%Y-%m-%d %H:%M:%S"),
                        "device":     device,
                        "city":       city,
                        "source":     source,
                    })
            else:
                if random.random() < 0.40:
                    events.append({
                        "event_id":   fake.uuid4(),
                        "user_id":    user_id,
                        "event_type": "ghosted_by_recruiter",
                        "created_at": random_ts(current_ts, 60, 4320).strftime("%Y-%m-%d %H:%M:%S"),
                        "device":     device,
                        "city":       city,
                        "source":     source,
                    })

            # Interview invites during return sessions
            if got_invite and random.random() < 0.35:
                events.append({
                    "event_id":   fake.uuid4(),
                    "user_id":    user_id,
                    "event_type": "interview_invite",
                    "created_at": random_ts(current_ts, 120, 2880).strftime("%Y-%m-%d %H:%M:%S"),
                    "device":     device,
                    "city":       city,
                    "source":     source,
                })
            elif not was_ghosted and random.random() < 0.15:
                events.append({
                    "event_id":   fake.uuid4(),
                    "user_id":    user_id,
                    "event_type": "interview_invite",
                    "created_at": random_ts(current_ts, 120, 2880).strftime("%Y-%m-%d %H:%M:%S"),
                    "device":     device,
                    "city":       city,
                    "source":     source,
                })

    return events


def generate_user_journey(user_id: str, cohort_date: datetime) -> list[dict]:
    events = []
    current_time = cohort_date + timedelta(
        hours=random.randint(7, 22),
        minutes=random.randint(0, 59),
    )

    device = random.choice(["mobile", "desktop", "tablet"])
    city   = fake.city()
    source = random.choice(["organic", "paid_ad", "referral", "email"])

    got_invite   = False
    was_ghosted  = False
    reached_steps = []

    def emit(event_type, ts):
        events.append({
            "event_id":   fake.uuid4(),
            "user_id":    user_id,
            "event_type": event_type,
            "created_at": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "device":     device,
            "city":       city,
            "source":     source,
        })

    # ── Initial funnel journey ─────────────────────────────────────────────────
    for step in FUNNEL_STEPS:
        emit(step, current_time)
        reached_steps.append(step)
        current_time = random_ts(current_time, 2, 120)

        if step == "job_view" and random.random() < 0.45:
            emit("irrelevant_job_shown", random_ts(current_time, 1, 30))

        if step == "job_apply":
            if random.random() < 0.60:
                was_ghosted = True
                emit("ghosted_by_recruiter", random_ts(current_time, 60, 4320))

        if step == "interview_invite":
            got_invite = True

        if step in CONTINUE_PROB:
            if random.random() > CONTINUE_PROB[step]:
                break

    # ── Return sessions — behavior driven by outcome ───────────────────────────
    if "signup" in reached_steps:
        return_events = generate_return_sessions(
            user_id=user_id,
            first_event_date=cohort_date,
            device=device,
            city=city,
            source=source,
            num_weeks=11,
            got_invite=got_invite,
            was_ghosted=was_ghosted,
        )
        events.extend(return_events)

    return events


def generate_churned_user(user_id: str, cohort_date: datetime) -> list[dict]:
    """Fully churned user — signed up, did nothing, never returned."""
    events = []
    ts     = cohort_date - timedelta(days=random.randint(15, 45))
    device = random.choice(["mobile", "desktop"])
    city   = fake.city()
    source = random.choice(["organic", "paid_ad"])

    def emit(event_type, t):
        events.append({
            "event_id":   fake.uuid4(),
            "user_id":    user_id,
            "event_type": event_type,
            "created_at": t.strftime("%Y-%m-%d %H:%M:%S"),
            "device":     device,
            "city":       city,
            "source":     source,
        })

    steps = random.choice([
        ["session_start", "signup"],
        ["session_start", "signup", "profile_complete"],
        ["session_start", "signup", "profile_complete", "job_search"],
    ])
    for s in steps:
        emit(s, ts)
        ts = random_ts(ts, 5, 120)

    return events


def main():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    all_events   = []
    base_date    = datetime(2024, 1, 1)
    user_counter = 0

    for week in range(12):
        cohort_date = base_date + timedelta(weeks=week)
        cohort_size = random.randint(180, 260)
        for _ in range(cohort_size):
            uid = f"U{user_counter:05d}"
            user_counter += 1
            all_events.extend(generate_user_journey(uid, cohort_date))

    for _ in range(320):
        uid = f"U{user_counter:05d}"
        user_counter += 1
        all_events.extend(
            generate_churned_user(uid, base_date + timedelta(weeks=12))
        )

    fieldnames = [
        "event_id", "user_id", "event_type",
        "created_at", "device", "city", "source",
    ]
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_events)

    print(
        f"✅ Generated {len(all_events):,} events "
        f"for {user_counter:,} users → {OUTPUT_PATH}"
    )


if __name__ == "__main__":
    main()