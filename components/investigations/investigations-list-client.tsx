"use client";

import { useState } from "react";
import { InvestigationTable } from "@/components/investigations/investigation-table";
import { InvestigationFilterBar, FilterValues } from "@/components/investigations/investigation-filter-bar";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { RefreshCw, ChevronLeft, ChevronRight } from "lucide-react";
import type { InvestigationsPageData } from "@/types/bid";

export function InvestigationsListClient({ 
  initialData 
}: { 
  initialData: InvestigationsPageData 
}) {
  const [data, setData] = useState<InvestigationsPageData>(initialData);
  const [filters, setFilters] = useState<FilterValues>({});
  const [isLoading, setIsLoading] = useState(false);
  const [page, setPage] = useState(1);

  const fetchInvestigations = async (currentFilters: FilterValues, currentPage: number) => {
    setIsLoading(true);
    try {
      const params = new URLSearchParams();
      params.append("page", currentPage.toString());
      params.append("pageSize", "25");
      
      Object.entries(currentFilters).forEach(([key, value]) => {
        if (value) params.append(key, value);
      });

      const response = await fetch(`/api/investigations?${params.toString()}`);
      if (!response.ok) throw new Error("Failed to fetch investigations");
      const newData = await response.json();
      setData(newData);
      setPage(currentPage);
    } catch (error) {
      console.error("Error fetching investigations:", error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleFilterChange = (newFilters: FilterValues) => {
    setFilters(newFilters);
    void fetchInvestigations(newFilters, 1);
  };

  const handlePageChange = (newPage: number) => {
    void fetchInvestigations(filters, newPage);
  };

  const totalPages = Math.ceil(data.total / data.pageSize);

  return (
    <div className="space-y-6">
      <InvestigationFilterBar onFilterChange={handleFilterChange} initialFilters={filters} />

      <Card className="shadow-sm border-slate-200">
        <CardHeader className="flex flex-row items-center justify-between space-y-0">
          <div>
            <CardTitle>Recent Stored Investigations</CardTitle>
            <CardDescription>
              Showing {data.items.length} of {data.total} records {Object.keys(filters).length > 0 ? "matching your filters" : ""}.
            </CardDescription>
          </div>
          <div className="flex items-center gap-2">
            {isLoading && <RefreshCw className="h-4 w-4 animate-spin text-slate-400" />}
            <div className="flex items-center border border-slate-200 rounded-md overflow-hidden bg-white">
              <Button
                variant="ghost"
                size="default"
                className="h-8 w-8 rounded-none border-r border-slate-200 px-0"
                onClick={() => handlePageChange(page - 1)}
                disabled={page <= 1 || isLoading}
              >
                <ChevronLeft className="h-4 w-4" />
              </Button>
              <span className="px-3 text-xs font-medium text-slate-600">
                Page {page} of {totalPages || 1}
              </span>
              <Button
                variant="ghost"
                size="default"
                className="h-8 w-8 rounded-none border-l border-slate-200 px-0"
                onClick={() => handlePageChange(page + 1)}
                disabled={page >= totalPages || isLoading}
              >
                <ChevronRight className="h-4 w-4" />
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <InvestigationTable items={data.items} />
          
          {totalPages > 1 && (
            <div className="mt-6 flex items-center justify-center gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => handlePageChange(page - 1)}
                disabled={page <= 1 || isLoading}
              >
                Previous
              </Button>
              <div className="flex items-center gap-1 mx-2">
                {[...Array(Math.min(5, totalPages))].map((_, i) => {
                  let pageNum: number;
                  if (totalPages <= 5) pageNum = i + 1;
                  else if (page <= 3) pageNum = i + 1;
                  else if (page >= totalPages - 2) pageNum = totalPages - 4 + i;
                  else pageNum = page - 2 + i;
                  
                  return (
                    <Button
                      key={pageNum}
                      variant={page === pageNum ? "default" : "ghost"}
                      size="sm"
                      className="h-8 w-8 p-0"
                      onClick={() => handlePageChange(pageNum)}
                      disabled={isLoading}
                    >
                      {pageNum}
                    </Button>
                  );
                })}
              </div>
              <Button
                variant="outline"
                size="sm"
                onClick={() => handlePageChange(page + 1)}
                disabled={page >= totalPages || isLoading}
              >
                Next
              </Button>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
