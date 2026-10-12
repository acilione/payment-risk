import type { ShowcaseReport } from "./types";
import { profiles } from "./profiles";
export function RunConfiguration({
  config,
}: {
  config: ShowcaseReport["config"];
}) {
  return (
    <section className="card showcase-settings">
      <div className="card-heading">
        <h2>Generation settings</h2>
        <span>Seed {config.seed}</span>
      </div>
      <dl>
        <div>
          <dt>Customers</dt>
          <dd>{config.customers}</dd>
        </div>
        <div>
          <dt>Payments per customer</dt>
          <dd>
            {config.min_payments} to {config.max_payments}
          </dd>
        </div>
        <div>
          <dt>Simulated history</dt>
          <dd>{config.history_minutes} minutes</dd>
        </div>
        <div>
          <dt>Publish rate</dt>
          <dd>{config.publish_rate}/second</dd>
        </div>
        <div>
          <dt>Retry probability</dt>
          <dd>{config.retry_percent}%</dd>
        </div>
      </dl>
      <h3>Customer behavior weights</h3>
      {Object.entries(config.profiles).map(([profile, weight]) => (
        <div className="profile-weight" key={profile}>
          <span>{profiles[profile]?.name || profile}</span>
          <div>
            <i style={{ width: `${weight}%` }} />
          </div>
          <b>{weight}%</b>
        </div>
      ))}
      <p className="showcase-note">
        Weights are sampling probabilities. The seed reproduces amounts,
        behavior and relative timing; each run receives new IDs and a current
        time anchor.
      </p>
    </section>
  );
}
