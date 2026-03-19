-- Fix missing columns in wines table that cause 500 errors
-- The SQLAlchemy model defines these columns but they don't exist in the actual DB table

-- Add missing columns (IF NOT EXISTS prevents errors if already added)
DO $$
BEGIN
    -- Add needs_review column
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='wines' AND column_name='needs_review') THEN
        ALTER TABLE wines ADD COLUMN needs_review BOOLEAN DEFAULT FALSE;
        RAISE NOTICE 'Added needs_review column';
    END IF;
    
    -- Add review_notes column
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='wines' AND column_name='review_notes') THEN
        ALTER TABLE wines ADD COLUMN review_notes TEXT;
        RAISE NOTICE 'Added review_notes column';
    END IF;
    
    -- Add updated_at column
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='wines' AND column_name='updated_at') THEN
        ALTER TABLE wines ADD COLUMN updated_at TIMESTAMP;
        RAISE NOTICE 'Added updated_at column';
    END IF;
END $$;

-- Verify
SELECT column_name, data_type FROM information_schema.columns WHERE table_name='wines' ORDER BY ordinal_position;
