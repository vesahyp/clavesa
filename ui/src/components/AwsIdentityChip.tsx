/**
 * AwsIdentityChip — shows which AWS account / profile the UI server is
 * operating as, and lets you switch it.
 *
 * When a preview or run 403s on a cross-account bucket, the fast
 * diagnosis is "which account is the server even using?". This puts
 * that answer in the header — and makes it a switcher: picking a
 * different ~/.aws profile persists the choice and restarts the server
 * with it (the AWS SDK clients are built once at startup and can't be
 * hot-swapped).
 *
 * Hidden only when there's no AWS identity AND no profiles to pick —
 * pure local development with no AWS config at all.
 *
 * Backed by GET /api/runtime/identity and GET/PUT /api/workspace/aws-profile.
 */

import { useState } from "react";
import { Cloud } from "lucide-react";
import { toast } from "sonner";

import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectTrigger,
} from "@/components/ui/select";
import { cn } from "@/lib/utils";
import {
  setAWSProfile,
  useAWSProfile,
  useRuntimeIdentity,
  waitForServerReady,
} from "@/lib/queries";

// Sentinel for "no profile override — ambient / default credential
// chain". Radix Select reserves the empty string, so the clear option
// needs a real value.
const AMBIENT = "__ambient__";

export function AwsIdentityChip() {
  const { data: identity } = useRuntimeIdentity();
  const { data: profileData } = useAWSProfile();
  const [switching, setSwitching] = useState(false);

  const profiles = profileData?.profiles ?? [];
  const available = identity?.available ?? false;

  // Nothing to show and nothing to pick — pure local dev. Stay hidden.
  if (!available && profiles.length === 0) return null;

  // Trigger text describes what the server is *actually* running as
  // (identity); the dropdown's selected row reflects what's *persisted*
  // (profileData). They agree unless the file was changed since the
  // server last started.
  const display = available
    ? `${identity?.profile || "default"} · ${identity?.account_id}`
    : "no AWS identity";
  const selected = profileData?.profile ? profileData.profile : AMBIENT;

  async function onChange(value: string) {
    const profile = value === AMBIENT ? "" : value;
    setSwitching(true);
    try {
      await setAWSProfile(profile);
      toast.info(
        `Switching to ${profile || "the default credential chain"} — restarting the server…`,
      );
      await waitForServerReady();
      window.location.reload();
    } catch (err) {
      setSwitching(false);
      toast.error(
        err instanceof Error ? err.message : "Failed to switch AWS profile",
      );
    }
  }

  return (
    <Select value={selected} onValueChange={onChange} disabled={switching}>
      <SelectTrigger
        aria-label="AWS profile"
        className={cn(
          "h-7 w-auto gap-1.5 border-transparent bg-transparent px-2 text-xs",
          "text-muted-foreground shadow-none hover:bg-accent hover:text-foreground",
          "focus:ring-1 focus:ring-offset-0",
        )}
      >
        <Cloud className="h-3.5 w-3.5 shrink-0" />
        <span className="font-mono">{switching ? "switching…" : display}</span>
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          <SelectLabel>AWS profile — switching restarts the server</SelectLabel>
          <SelectItem value={AMBIENT}>default credential chain</SelectItem>
          {profiles.map((p) => (
            <SelectItem key={p} value={p}>
              {p}
            </SelectItem>
          ))}
        </SelectGroup>
      </SelectContent>
    </Select>
  );
}
