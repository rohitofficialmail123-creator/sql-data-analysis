/*
 * Frontline & Final Review Resolution Metrics — Multi-Region
 *
 * Purpose: Extract resolved case data with root cause and reimbursement details
 *          for frontline and final review queues across multiple regions.
 *
 * Business Context:
 *   Operations managers needed monthly resolution data broken down by region
 *   (NA and EU) and review stage (frontline vs final review) to track
 *   reimbursement accuracy, reopen rates, and root cause distribution.
 *   These 4 query variants were run monthly and fed into WBR dashboards.
 *
 * Techniques:
 *   - INNER JOIN between case management and decision tables
 *   - Date range filtering for monthly reporting windows
 *   - Queue-based segmentation for regional and stage-level analysis
 */


-- ============================================================
-- VARIANT 1: North America — Frontline Resolutions
-- ============================================================
SELECT
    c.case_id,
    c.queue_name,
    c.resolve_date,
    c.is_reopen,
    c.reopen_count,
    c.case_status,
    d.root_cause,
    d.credit_recommendation,
    d.suggested_reimbursement,
    d.final_reimbursement_amount
FROM
    case_management.cases c
    INNER JOIN case_management.decisions d ON c.case_id = d.case_id
WHERE
    c.resolve_date BETWEEN '2025-02-02' AND '2025-03-01'
    AND c.queue_name IN (
        'region-ca-frontline-en@example.com',
        'region-us-frontline-en@example.com',
        'region-us-frontline-tier2@example.com',
        'frontline-active-learning@example.com',
        'frontline-high-risk@example.com',
        'frontline-tier-300-2999-invoice@example.com',
        'frontline-tier-300-2999@example.com',
        'frontline-tier-3000@example.com',
        'frontline-tier-3000-m@example.com',
        'frontline-tier-3000-tier2@example.com',
        'frontline-cn-300-2999@example.com',
        'frontline-cn-3000@example.com',
        'frontline-cn-3000-m@example.com'
    );


-- ============================================================
-- VARIANT 2: North America — Final Review Resolutions
-- ============================================================
SELECT
    c.case_id,
    c.queue_name,
    c.resolve_date,
    c.is_reopen,
    c.reopen_count,
    c.case_status,
    d.root_cause,
    d.credit_recommendation,
    d.suggested_reimbursement,
    d.final_reimbursement_amount
FROM
    case_management.cases c
    INNER JOIN case_management.decisions d ON c.case_id = d.case_id
WHERE
    c.resolve_date BETWEEN '2025-02-02' AND '2025-03-01'
    AND c.queue_name IN (
        'hv-final-review@example.com',
        'hv-final-word@example.com'
    );


-- ============================================================
-- VARIANT 3: Europe — Frontline Resolutions (5 marketplaces)
-- ============================================================
SELECT
    c.case_id,
    c.queue_name,
    c.resolve_date,
    c.is_reopen,
    c.reopen_count,
    c.case_status,
    d.root_cause,
    d.credit_recommendation,
    d.suggested_reimbursement,
    d.final_reimbursement_amount
FROM
    case_management.cases c
    INNER JOIN case_management.decisions d ON c.case_id = d.case_id
WHERE
    c.resolve_date BETWEEN '2025-02-02' AND '2025-03-01'
    AND c.queue_name IN (
        -- UK queues
        'frontline-en-500-2499@example.co.uk',
        'frontline-en-2500@example.co.uk',
        'frontline-cn-500-2499@example.co.uk',
        'frontline-cn-2500@example.co.uk',
        'frontline-tier2-en@example.co.uk',
        'frontline-tier2-cn@example.co.uk',
        -- DE queues
        'frontline-en-500-2499@example.de',
        'frontline-en-2500@example.de',
        'frontline-cn-500-2499@example.de',
        'frontline-cn-2500@example.de',
        'frontline-de-500-2499@example.de',
        'frontline-de-2500@example.de',
        -- FR queues
        'frontline-en-500-2499@example.fr',
        'frontline-en-2500@example.fr',
        'frontline-cn-500-2499@example.fr',
        'frontline-cn-2500@example.fr',
        'frontline-fr-500-2499@example.fr',
        'frontline-fr-2500@example.fr',
        -- IT queues
        'frontline-en-500-2499@example.it',
        'frontline-en-2500@example.it',
        'frontline-cn-500-2499@example.it',
        'frontline-cn-2500@example.it',
        -- ES queues
        'frontline-en-500-2499@example.es',
        'frontline-en-2500@example.es',
        'frontline-cn-500-2499@example.es',
        'frontline-cn-2500@example.es'
    );


-- ============================================================
-- VARIANT 4: Europe — Final Review Resolutions (5 marketplaces)
-- ============================================================
SELECT
    c.case_id,
    c.queue_name,
    c.resolve_date,
    c.is_reopen,
    c.reopen_count,
    c.case_status,
    d.root_cause,
    d.credit_recommendation,
    d.suggested_reimbursement,
    d.final_reimbursement_amount
FROM
    case_management.cases c
    INNER JOIN case_management.decisions d ON c.case_id = d.case_id
WHERE
    c.resolve_date BETWEEN '2025-02-02' AND '2025-03-01'
    AND c.queue_name IN (
        'final-review-lv@example.co.uk',
        'final-review-hv@example.co.uk',
        'hv-final-word@example.co.uk',
        'hv-final-word@example.de',
        'hv-final-word@example.fr',
        'hv-final-word@example.it',
        'hv-final-word@example.es'
    );
