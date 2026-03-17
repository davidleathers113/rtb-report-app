"use client";

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ROOT_CAUSES, OWNER_TYPES, OUTCOMES } from "@/types/bid";
import { toSentenceCase } from "@/lib/utils";
import { Search, X, Filter } from "lucide-react";

export interface FilterValues {
  search?: string;
  startDate?: string;
  endDate?: string;
  publisherName?: string;
  campaignName?: string;
  outcome?: string;
  rootCause?: string;
  ownerType?: string;
}

interface InvestigationFilterBarProps {
  onFilterChange: (filters: FilterValues) => void;
  initialFilters?: FilterValues;
}

export function InvestigationFilterBar({
  onFilterChange,
  initialFilters = {},
}: InvestigationFilterBarProps) {
  const [filters, setFilters] = useState<FilterValues>(initialFilters);
  const [isExpanded, setIsExpanded] = useState(false);

  const updateFilter = (key: keyof FilterValues, value: string) => {
    const newFilters = { ...filters, [key]: value || undefined };
    setFilters(newFilters);
  };

  const handleApply = () => {
    onFilterChange(filters);
  };

  const handleClear = () => {
    const cleared = {};
    setFilters(cleared);
    onFilterChange(cleared);
  };

  const activeFilterCount = Object.values(filters).filter(Boolean).length;

  return (
    <div className="bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
      <div className="p-4 flex flex-wrap items-center gap-4">
        <div className="relative flex-1 min-w-[240px]">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-slate-400" />
          <Input
            placeholder="Search Bid ID, Campaign, Publisher, or Error..."
            value={filters.search || ""}
            onChange={(e) => updateFilter("search", e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleApply()}
            className="pl-10"
          />
        </div>

        <div className="flex items-center gap-2">
          <Button 
            variant="outline" 
            onClick={() => setIsExpanded(!isExpanded)}
            className={isExpanded ? "bg-slate-50" : ""}
          >
            <Filter className="mr-2 h-4 w-4" />
            Advanced Filters
            {activeFilterCount > 0 && (
              <span className="ml-2 px-1.5 py-0.5 bg-sky-100 text-sky-700 text-xs rounded-full">
                {activeFilterCount}
              </span>
            )}
          </Button>
          <Button onClick={handleApply}>Apply Filters</Button>
          <Button
            variant="ghost"
            onClick={handleClear}
            size="default"
            className="h-10 w-10 px-0"
            title="Clear all"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>
      </div>

      {isExpanded && (
        <div className="p-4 border-t border-slate-100 bg-slate-50/50 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <div className="space-y-1.5">
            <label className="text-xs font-semibold text-slate-500 uppercase tracking-wider">
              Start Date
            </label>
            <Input
              type="datetime-local"
              value={filters.startDate ? filters.startDate.slice(0, 16) : ""}
              onChange={(e) => {
                const val = e.target.value;
                updateFilter("startDate", val ? new Date(val).toISOString() : "");
              }}
            />
          </div>

          <div className="space-y-1.5">
            <label className="text-xs font-semibold text-slate-500 uppercase tracking-wider">
              End Date
            </label>
            <Input
              type="datetime-local"
              value={filters.endDate ? filters.endDate.slice(0, 16) : ""}
              onChange={(e) => {
                const val = e.target.value;
                updateFilter("endDate", val ? new Date(val).toISOString() : "");
              }}
            />
          </div>

          <div className="space-y-1.5">
            <label className="text-xs font-semibold text-slate-500 uppercase tracking-wider">
              Publisher
            </label>
            <Input
              placeholder="Publisher Name"
              value={filters.publisherName || ""}
              onChange={(e) => updateFilter("publisherName", e.target.value)}
            />
          </div>

          <div className="space-y-1.5">
            <label className="text-xs font-semibold text-slate-500 uppercase tracking-wider">
              Campaign
            </label>
            <Input
              placeholder="Campaign Name"
              value={filters.campaignName || ""}
              onChange={(e) => updateFilter("campaignName", e.target.value)}
            />
          </div>

          <div className="space-y-1.5">
            <label className="text-xs font-semibold text-slate-500 uppercase tracking-wider">
              Outcome
            </label>
            <select
              className="w-full h-10 px-3 rounded-md border border-slate-200 bg-white text-sm"
              value={filters.outcome || ""}
              onChange={(e) => updateFilter("outcome", e.target.value)}
            >
              <option value="">All Outcomes</option>
              {OUTCOMES.map((o) => (
                <option key={o} value={o}>{toSentenceCase(o)}</option>
              ))}
            </select>
          </div>

          <div className="space-y-1.5">
            <label className="text-xs font-semibold text-slate-500 uppercase tracking-wider">
              Root Cause
            </label>
            <select
              className="w-full h-10 px-3 rounded-md border border-slate-200 bg-white text-sm"
              value={filters.rootCause || ""}
              onChange={(e) => updateFilter("rootCause", e.target.value)}
            >
              <option value="">All Root Causes</option>
              {ROOT_CAUSES.map((rc) => (
                <option key={rc} value={rc}>{toSentenceCase(rc)}</option>
              ))}
            </select>
          </div>

          <div className="space-y-1.5">
            <label className="text-xs font-semibold text-slate-500 uppercase tracking-wider">
              Owner Type
            </label>
            <select
              className="w-full h-10 px-3 rounded-md border border-slate-200 bg-white text-sm"
              value={filters.ownerType || ""}
              onChange={(e) => updateFilter("ownerType", e.target.value)}
            >
              <option value="">All Owner Types</option>
              {OWNER_TYPES.map((ot) => (
                <option key={ot} value={ot}>{toSentenceCase(ot)}</option>
              ))}
            </select>
          </div>
        </div>
      )}
    </div>
  );
}
