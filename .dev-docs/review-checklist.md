# Review Checklist

## Correctness

* Edge cases handled
* Errors are actionable and not swallowed

## Tests

* New behavior covered
* Tests are deterministic and fast enough

## Security

* No secrets in code/logs
* Inputs validated; authz/authn preserved

## Performance

* Avoid obvious N+1 / hot loops / excessive allocations

## Compatibility

* Public APIs unchanged or documented
* Backward-compatible migrations/config changes

## Observability

* Logs/metrics/traces reasonable (if applicable)

## Documentation

* README/docs updated if behavior changed
