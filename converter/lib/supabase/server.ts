import { createClient } from "@supabase/supabase-js";

/**
 * Server-only client met service role. Alleen gebruiken in Route Handlers / Server Actions
 * waar je RLS wilt omzeilen (bv. interne jobs). Niet importeren in client components.
 */
export function createServiceRoleClient() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) {
    throw new Error(
      "NEXT_PUBLIC_SUPABASE_URL en SUPABASE_SERVICE_ROLE_KEY zijn verplicht op de server.",
    );
  }
  return createClient(url, key, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
}
