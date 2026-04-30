/*
 * Queue Transfer Tracking — Incoming Volume to New Regional Queues
 *
 * Purpose: Track when cases were created and when they were transferred out
 *          to newly established regional queues, with timezone conversion
 *          for the local operations team.
 *
 * Business Context:
 *   When new regional queues were established across 11 marketplaces,
 *   leadership needed to monitor incoming case volume and transfer timing
 *   to ensure the new queues were being utilized correctly and to measure
 *   transfer latency (time from case creation to queue transfer).
 *
 * Techniques:
 *   - Multi-table JOIN between case data and event log
 *   - convert_timezone() for cross-timezone reporting (PST → IST, UTC → IST)
 *   - Event-type filtering on operational event log
 *   - Multi-marketplace queue filtering (11 regional queues)
 */

SELECT
    c.case_id,
    convert_timezone('America/Los_Angeles', 'Asia/Kolkata', c.creation_date_pst)
        AS creation_date_local,
    c.queue_name,
    convert_timezone('UTC', 'Asia/Kolkata', e.event_timestamp)
        AS transfer_date_local
FROM
    case_management.cases c
    JOIN case_management.case_events e
        ON c.case_id = e.case_id
WHERE
    c.queue_name IN (
        'final-review-en-us@example.com',
        'final-review-en-ca@example.ca',
        'final-review-en-mx@example.com.mx',
        'final-review-en-br@example.com.br',
        'final-review-en-au@example.com.au',
        'final-review-en-sg@example.com.sg',
        'final-review-en-uk@example.co.uk',
        'final-review-en-de@example.de',
        'final-review-en-fr@example.fr',
        'final-review-en-it@example.it',
        'final-review-en-es@example.es'
    )
    AND e.event_type = 'Transfer Out Of Queue'
    AND convert_timezone('UTC', 'Asia/Kolkata', e.event_timestamp)
        >= '2025-07-07 00:00:00';
