

CREATE TEMPORARY TABLE collection_loans_1 as (
SELECT
  ta.taggable_id as loan_id
  , CASE WHEN f.created_at is not NULL THEN f.created_at
    ELSE ta.updated_at END as assigned_at
  ,regexp_substr(CASE WHEN f.new_value is not NULL THEN f.new_value
    ELSE ta.value END, '"agency_name":\s*"([^"]*)"', 0, 1, 'e') as assigned_to
  , ta.created_at as originally_assigned_at
  , regexp_substr(f.old_value, '"agency_name":\s*"([^"]*)"', 0, 1, 'e') as prev_assigned_to
FROM
  tags ta
LEFT JOIN facts f ON f.item_id = ta.id
  AND f.item_type = 'Tag'
  AND f.attr_name = 'value'
WHERE
  ta.taggable_type = 'Loan'
  AND ta.name = 'servicing_agency'
);

CREATE TEMPORARY TABLE collection_loans as (
SELECT *
FROM
collection_loans_1
UNION
  ( -- create a row for the first outsourcing agency for loans that moved to another agency.
  SELECT
      cl.loan_id
      , CASE WHEN prev_assigned_to IS NOT NULL THEN originally_assigned_at ELSE assigned_at END AS assigned_at
      , CASE WHEN prev_assigned_to IS NOT NULL THEN prev_assigned_to ELSE assigned_to END AS assigned_to
      , originally_assigned_at
      , CASE when prev_assigned_to IS NOT NULL THEN NULL ELSE prev_assigned_to END AS prev_assigned_to
  FROM
      collection_loans_1 cl
  JOIN (
      SELECT
          cl3.loan_id
          , MIN(cl3.assigned_at) as min_assigned
      FROM
          collection_loans_1 cl3
      GROUP BY cl3.loan_id
    ) cl1
    ON cl.loan_id = cl1.loan_id AND cl1.min_assigned = cl.assigned_at
  )
);

CREATE TEMPORARY TABLE reassignments as (
SELECT
  cl.loan_id
  , cl.assigned_at
  , min(cl2.assigned_at) as reassigned_at
FROM
  collection_loans cl
LEFT JOIN collection_loans cl2 ON cl.assigned_to = cl2.prev_assigned_to
  AND cl2.assigned_at > cl.assigned_at
  AND cl.loan_id = cl2.loan_id
GROUP BY
  cl.loan_id
  , cl.assigned_at
  );


CREATE TEMPORARY TABLE loan_information AS (
    SELECT
        l.id AS loan_id
        , l.currency
        , case when l.status = 5 then lp.max_time_completed else NULL END AS paid_in_full_at
    FROM
        loans l
    LEFT JOIN (
      SELECT
        loan_id
        , MAX(time_completed) AS max_time_completed
      FROM transactions
      WHERE status = 2
        AND txn_type IN (2,5)
      GROUP BY loan_id
        ) lp
      ON lp.loan_id = l.id
);


CREATE TEMPORARY TABLE collections_transactions as (

WITH
scheduled_repayments_mod as (
  SELECT
      sr1.loan_id
      ,sr1.repayment_num
      ,sr1.date_scheduled
      ,sr1.date_repaid
      ,sr1.amount_principal + amount_fee as amt_due
      ,sr1.total_due as total_due_to_date
  FROM (
      SELECT
          sr.*
          ,row_number() over (partition by loan_id order by date_scheduled asc) as repayment_num
      FROM
          scheduled_repayments sr
        ) sr1
),

final_dd as (
  SELECT
    loan_id
    , max(total_due_to_date) as final_amount_due
    , max(date_scheduled) as final_repayment_date
    , max(repayment_num) as final_repayment_num
  FROM
    scheduled_repayments_mod
  GROUP BY
    1
)

SELECT
cl.loan_id
,cl.assigned_at
, case when eos.total_due_to_date is NULL then final_amount_due else eos.total_due_to_date end as total_due
, case when eos.prior_sch_date is NULL then final_repayment_date else eos.prior_sch_date end as missed_payment_at
, case when eos.repayment_num is NULL then final_repayment_num else eos.repayment_num end as missed_payment_num
, case when eos.next_sch_date is not NULL then TRUE else FALSE end as early_outsourcing_flag
, SUM(CASE WHEN tr.time_completed < cl.assigned_at then tr.amount else 0 END) as amt_prev_collected
, SUM(CASE WHEN tr.time_completed > cl.assigned_at and (
    (tr.time_completed < cr.reassigned_at OR cr.reassigned_at is NULL) and (tr.time_completed <= eos.date_repaid or eos.date_repaid is NULL)
    OR (tr.time_completed <= eos.date_repaid and (tr.time_completed < cr.reassigned_at OR cr.reassigned_at is NULL)))
    THEN tr.amount else 0 END) as amt_collected
, sum(CASE WHEN tr.time_completed > cl.assigned_at and (
    (tr.time_completed < cr.reassigned_at OR cr.reassigned_at is NULL) and (tr.time_completed <= eos.date_repaid or eos.date_repaid is NULL)
    OR (tr.time_completed <= eos.date_repaid and (tr.time_completed < cr.reassigned_at OR cr.reassigned_at is NULL)))
    THEN 1 else 0 END) as repayment_count
,min(CASE WHEN tr.time_completed > cl.assigned_at and (
    (tr.time_completed < cr.reassigned_at OR cr.reassigned_at is NULL) and (tr.time_completed <= eos.date_repaid or eos.date_repaid is NULL)
    OR (tr.time_completed <= eos.date_repaid and (tr.time_completed < cr.reassigned_at OR cr.reassigned_at is NULL)))
    THEN tr.time_completed else NULL END) as first_collected_at
,max(CASE WHEN tr.time_completed > cl.assigned_at and (
    (tr.time_completed < cr.reassigned_at OR cr.reassigned_at is NULL) and (tr.time_completed <= eos.date_repaid or eos.date_repaid is NULL)
    OR (tr.time_completed <= eos.date_repaid and (tr.time_completed < cr.reassigned_at OR cr.reassigned_at is NULL)))
    THEN tr.time_completed else NULL END) as last_collected_at
FROM
  collection_loans cl
LEFT JOIN transactions tr ON tr.loan_id = cl.loan_id
  AND tr.txn_type = 2
  AND tr.status in (2,5)
JOIN final_dd f ON f.loan_id = cl.loan_id
LEFT JOIN (
  SELECT
    sr1.loan_id
    ,sr1.date_scheduled as prior_sch_date
    ,sr1.date_repaid
    ,sr1.total_due_to_date
    ,sr1.repayment_num
    ,sr2.date_scheduled as next_sch_date
  FROM
    scheduled_repayments_mod sr1
  JOIN scheduled_repayments_mod sr2
    ON sr2.loan_id = sr1.loan_id
    AND sr2.repayment_num = sr1.repayment_num + 1
) eos
  ON eos.loan_id = cl.loan_id
  AND eos.prior_sch_date < cl.assigned_at
  AND eos.next_sch_date > cl.assigned_at
LEFT JOIN reassignments cr ON cr.loan_id = cl.loan_id
  AND cr.assigned_at = cl.assigned_at
GROUP BY
  1,2,3,4,5,6
);


CREATE TEMPORARY TABLE collection_stats_temp2 as (
SELECT
  cl.loan_id
  , li.currency
  , cl.assigned_at
  , cl.assigned_to
  , ROW_NUMBER() OVER (PARTITION BY cl.loan_id ORDER BY cl.assigned_at) as assignment_rank
  , ct.total_due
  -- below is to control for cases where we outsource after the loan is already paid in full
  , case when ct.total_due - coalesce(ct.amt_prev_collected,0) <= 0 then null else ct.total_due - coalesce(ct.amt_prev_collected,0) end as amt_assigned
  , coalesce(ct.amt_collected,0) as amt_collected
  , coalesce(ct.repayment_count,0) as repayment_count
  , ct.first_collected_at
  , ct.last_collected_at
  , li.paid_in_full_at
  , case when sub.sub_rank is not null then TRUE else FALSE end as currently_assigned
  , case when early_outsourcing_flag is null then FALSE else early_outsourcing_flag end as early_outsourcing_flag---Need to make based on the loan_id and applied across all rows of this loan
  ---Should add a removed from collections field date for last row.
FROM
  collection_loans cl
LEFT JOIN reassignments r ON cl.loan_id = r.loan_id
  AND r.assigned_at = cl.assigned_at
LEFT JOIN collections_transactions ct ON cl.loan_id = ct.loan_id
  AND cl.assigned_at = ct.assigned_at
JOIN loan_information li ON li.loan_id = cl.loan_id
LEFT JOIN (
    SELECT
      loan_id
      , assigned_at
      , assigned_to
      , ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY assigned_at desc) as sub_rank
    FROM
      collection_loans) sub
  ON sub.loan_id = cl.loan_id
  AND sub.assigned_at = cl.assigned_at
  AND sub.assigned_to = cl.assigned_to
  AND sub.sub_rank = 1
LEFT JOIN (
	SELECT
	  loan_id
	FROM
	  collections_transactions
	WHERE
	  early_outsourcing_flag is TRUE) eos
  on eos.loan_id = cl.loan_id
WHERE
  cl.assigned_to <> 'removed_from_collections'
  AND ct.total_due is NOT NULL
  AND assigned_amt is NOT NULL
  )
;

CREATE TABLE development.collection_stats_pf as (
SELECT
 cl1.*
 , case when removal.sub_rank is not null then TRUE else FALSE end as removed_from_collections_after
FROM
  collection_stats_temp2 cl1
LEFT JOIN (
    SELECT
      loan_id
      , assigned_at
      , assigned_to
      , ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY assigned_at) as sub_rank
    FROM
      collection_loans) removal
  ON removal.loan_id = cl1.loan_id
  AND removal.sub_rank = cl1.assignment_rank + 1
  AND removal.assigned_to = 'removed_from_collections'
  )
;
