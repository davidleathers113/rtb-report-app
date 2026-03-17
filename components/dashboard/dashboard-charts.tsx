"use client";

import { useEffect, useRef, useState, type ReactNode } from "react";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import type { DashboardMetric, DashboardTimePoint } from "@/types/bid";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { toSentenceCase } from "@/lib/utils";

const pieColors = ["#0ea5e9", "#f97316", "#10b981", "#8b5cf6", "#ef4444"];

function formatMetricLabel(value: string) {
  return toSentenceCase(value);
}

function ChartFrame({ children }: { children: (size: { width: number; height: number }) => ReactNode }) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [size, setSize] = useState({
    width: 0,
    height: 0,
  });

  useEffect(() => {
    const container = containerRef.current;

    if (!container) {
      return;
    }

    const updateSize = () => {
      const nextWidth = Math.floor(container.clientWidth);
      const nextHeight = Math.floor(container.clientHeight);

      setSize((current) => {
        if (current.width === nextWidth && current.height === nextHeight) {
          return current;
        }

        return {
          width: nextWidth,
          height: nextHeight,
        };
      });
    };

    updateSize();

    const observer = new ResizeObserver(() => {
      updateSize();
    });
    observer.observe(container);

    return () => {
      observer.disconnect();
    };
  }, []);

  const canRenderChart = size.width > 0 && size.height > 0;

  return (
    <div ref={containerRef} className="h-full w-full">
      {canRenderChart ? children(size) : <div className="h-full w-full rounded-lg bg-slate-100" />}
    </div>
  );
}

export function DashboardCharts({
  errorsByCategory,
  bidsByOutcome,
  issuesOverTime,
}: {
  errorsByCategory: DashboardMetric[];
  bidsByOutcome: DashboardMetric[];
  issuesOverTime: DashboardTimePoint[];
}) {
  return (
    <div className="grid gap-6 lg:grid-cols-2">
      <Card>
        <CardHeader>
          <CardTitle>Errors By Category</CardTitle>
        </CardHeader>
        <CardContent className="h-80">
          <ChartFrame>
            {({ width, height }) => (
              <BarChart width={width} height={height} data={errorsByCategory}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} />
                <XAxis
                  dataKey="label"
                  angle={-20}
                  height={72}
                  interval={0}
                  textAnchor="end"
                  tick={{ fontSize: 12 }}
                  tickFormatter={formatMetricLabel}
                />
                <YAxis allowDecimals={false} />
                <Tooltip
                  formatter={(value) => [value, "Count"]}
                  labelFormatter={(label) => formatMetricLabel(String(label))}
                />
                <Bar dataKey="value" fill="#0ea5e9" radius={[6, 6, 0, 0]} />
              </BarChart>
            )}
          </ChartFrame>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Bids By Outcome</CardTitle>
        </CardHeader>
        <CardContent className="h-80">
          <ChartFrame>
            {({ width, height }) => (
              <PieChart width={width} height={height}>
                <Pie
                  data={bidsByOutcome}
                  dataKey="value"
                  nameKey="label"
                  cx="50%"
                  cy="50%"
                  outerRadius={90}
                  label
                >
                  {bidsByOutcome.map((entry, index) => (
                    <Cell
                      key={entry.label}
                      fill={pieColors[index % pieColors.length]}
                    />
                  ))}
                </Pie>
                <Tooltip />
                <Legend />
              </PieChart>
            )}
          </ChartFrame>
        </CardContent>
      </Card>

      <Card className="lg:col-span-2">
        <CardHeader>
          <CardTitle>Issues Over Time</CardTitle>
        </CardHeader>
        <CardContent className="h-80">
          <ChartFrame>
            {({ width, height }) => (
              <LineChart width={width} height={height} data={issuesOverTime}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="date" tick={{ fontSize: 12 }} />
                <YAxis allowDecimals={false} />
                <Tooltip />
                <Legend />
                <Line
                  type="monotone"
                  dataKey="total"
                  stroke="#0f172a"
                  strokeWidth={2}
                />
                <Line
                  type="monotone"
                  dataKey="rejected"
                  stroke="#ef4444"
                  strokeWidth={2}
                />
                <Line
                  type="monotone"
                  dataKey="zeroBid"
                  stroke="#f59e0b"
                  strokeWidth={2}
                />
              </LineChart>
            )}
          </ChartFrame>
        </CardContent>
      </Card>
    </div>
  );
}
