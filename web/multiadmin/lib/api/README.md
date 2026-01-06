# MultiAdmin API Client

Auto-generated TypeScript client from Protocol Buffers using [protoc-gen-grpc-gateway-ts](https://github.com/grpc-ecosystem/protoc-gen-grpc-gateway-ts).

## Files

- **`generated/*.pb.ts`** - Generated code (DO NOT EDIT). Regenerate with `make proto-ts`.
- **`client.ts`** - Instance-based API wrapper
- **`context.tsx`** - React context provider
- **`index.ts`** - Public exports

## Usage

```typescript
import { useApi } from "@/lib/api";

function MyComponent() {
  const api = useApi();

  // Methods with no required fields can omit the request object
  const { names } = await api.getDatabaseNames();

  // Pass request object when filtering
  const { poolers } = await api.getPoolers({ cells: ["zone1"] });
}
```

## Field Naming

Generated types use **camelCase** (e.g., `tableGroup`, `servingStatus`, `portMap`).
JSON wire format uses **snake_case** (per gRPC-gateway config).

## Regenerating

After modifying `.proto` files:

```bash
make proto-ts      # Regenerate TypeScript only
make build-all     # Regenerate everything (recommended)
```

Generated files are committed to the repository.
