-- The decider's reason, carried from the approve/reject call to the resumed
-- branch so the operation can act on it (ctx.approval_reason), the way an input
-- gate's answer reaches ctx.input_answer. Cleared with the rest of the approval
-- fields once the run advances; the audit event keeps the durable copy.
ALTER TABLE jobs ADD COLUMN approval_reason TEXT NOT NULL DEFAULT '';
