ALTER TABLE public.cetus_swap
ADD COLUMN amount_a_in NUMERIC NOT NULL DEFAULT 0;

ALTER TABLE public.cetus_swap
ADD COLUMN amount_a_out NUMERIC NOT NULL DEFAULT 0;

ALTER TABLE public.cetus_swap
ADD COLUMN amount_b_in NUMERIC NOT NULL DEFAULT 0;

ALTER TABLE public.cetus_swap
ADD COLUMN amount_b_out NUMERIC NOT NULL DEFAULT 0;
