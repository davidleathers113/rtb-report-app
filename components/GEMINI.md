# UI Components (`components/`)

This directory contains the React components for the RTB Report App.

## Component Structure

- **`shared/`:** Reusable UI components (e.g., `Badge`, `Button`, `Card`, `Table`). These are primarily built using Tailwind CSS and Radix UI primitives.
- **`investigations/`:** Feature-specific components for the bid investigation console.
- **`layout/`:** High-level layout components like `AppShell`.

## Tech Stack & Styling

- **Tailwind CSS (v4):** Use Tailwind for all styling. Avoid custom CSS files unless absolutely necessary.
- **Radix UI:** Use Radix primitives for accessible and robust UI elements (e.g., `Slot`, `Dialog`, `Select`).
- **Lucide Icons:** Use `lucide-react` for consistent iconography.

## Design Patterns

- **"use client" Directive:** Mark all interactive components that use React hooks (state, effect, context) as client components.
- **State Management:** For complex features like bulk investigations, centralize state in a large client component (`BulkInvestigationClient.tsx`).
- **Polling:** Use `useEffect` with `setTimeout` or `setInterval` for polling the progress of asynchronous backend operations (e.g., `ImportRun` processing).
- **Conditional Rendering:** Use semantic badges and colors (e.g., `success`, `warning`, `destructive`) to represent the status of runs, items, and system health.
- **JSON View:** Use the `JsonView` component for displaying raw trace data and other complex objects in a readable format.

## Guidelines

- **Input Validation:** Use the `parseBidIds` utility to validate and deduplicate user input before sending it to the API.
- **Error Display:** Always display a clear error message to the user if an API request fails.
- **Progress Indicators:** Provide visual feedback (e.g., progress bars, status labels) for long-running operations.
- **Accessibility:** Ensure all components are keyboard-accessible and have appropriate ARIA attributes, especially for interactive elements.
