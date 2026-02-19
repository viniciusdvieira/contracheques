
-- Insert a default admin user for testing
-- This creates the admin role entry manually
-- The actual auth user must be created via the dashboard or signup

-- Note: Admin users should be created via Supabase Auth dashboard
-- This is just a placeholder comment for the schema

-- We'll ensure the has_role function works correctly
-- by adding a helper to promote any existing user to admin if needed

CREATE OR REPLACE FUNCTION public.promote_to_admin(_email TEXT)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  _user_id UUID;
BEGIN
  SELECT id INTO _user_id FROM auth.users WHERE email = _email;
  IF _user_id IS NULL THEN
    RAISE EXCEPTION 'User not found: %', _email;
  END IF;
  
  -- Upsert profile
  INSERT INTO public.profiles (id, full_name, must_change_password)
  VALUES (_user_id, 'Administrador', false)
  ON CONFLICT (id) DO UPDATE SET must_change_password = false;
  
  -- Insert admin role
  INSERT INTO public.user_roles (user_id, role)
  VALUES (_user_id, 'admin')
  ON CONFLICT (user_id, role) DO NOTHING;
END;
$$;
