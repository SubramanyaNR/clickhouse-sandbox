-- bankgen plants three problems in the data. Finding them needs more than
-- one stream, which is the point.

-- 1. Crash spike: payments errors by week, for the affected version and
-- platform against everything else. It starts, peaks and stops.
SELECT
    toStartOfWeek(event_date) AS week,
    round(100 * countIf(level = 'ERROR' AND app_version = '4.2.1' AND device = 'android')
              / countIf(app_version = '4.2.1' AND device = 'android'), 1) AS android_421_error_pct,
    round(100 * countIf(level = 'ERROR' AND NOT (app_version = '4.2.1' AND device = 'android'))
              / countIf(NOT (app_version = '4.2.1' AND device = 'android')), 1) AS everyone_else_pct
FROM bank.application_logs
WHERE module = 'payments'
GROUP BY week
ORDER BY week;

-- 2. During the same weeks the same users stop finishing EMI payments.
-- windowFunnel returns how far through the ordered steps each session got.
-- 4.2.1 on iOS is the control: same release, different platform.
SELECT
    app_version, device,
    count() AS sessions,
    round(100 * countIf(steps = 4) / count(), 1) AS completed_pct
FROM
(
    SELECT
        session_id,
        any(app_version) AS app_version,
        any(device) AS device,
        windowFunnel(1800)(toDateTime(event_time),
            screen = 'home', screen = 'loan_detail', screen = 'emi_payment', screen = 'payment_confirm') AS steps
    FROM bank.clickstream
    WHERE event_date BETWEEN '2026-08-18' AND '2026-09-01'
    GROUP BY session_id
    HAVING countIf(screen = 'loan_detail') > 0
)
GROUP BY app_version, device
ORDER BY completed_pct;

-- 3. A policy change: one rejection reason dominates for two products after
-- a date. bankgen anchors its data at 2026-09-08 and the change lands 45
-- days before that.
SELECT
    product,
    event_date >= '2026-07-25' AS after_change,
    countIf(rejection_reason_code = 'INCOME_VERIFICATION_FAIL') AS income_fail,
    countIf(decision = 'rejected') AS rejected,
    round(100 * income_fail / nullIf(rejected, 0), 1) AS income_fail_pct
FROM bank.loan_applications
GROUP BY product, after_change
ORDER BY product, after_change;
