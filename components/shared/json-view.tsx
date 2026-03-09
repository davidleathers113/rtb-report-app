import { stringifyJson } from "@/lib/utils/json";

export function JsonView({
  value,
  emptyLabel = "No data available.",
}: {
  value: unknown;
  emptyLabel?: string;
}) {
  const content = stringifyJson(value);

  if (!content) {
    return (
      <div className="rounded-lg border border-dashed border-slate-200 bg-slate-50 p-4 text-sm text-slate-500">
        {emptyLabel}
      </div>
    );
  }

  return (
    <pre className="max-h-[28rem] overflow-auto rounded-lg bg-slate-950 p-4 text-xs leading-6 text-slate-100">
      {content}
    </pre>
  );
}
