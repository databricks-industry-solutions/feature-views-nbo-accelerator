import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import {
  api,
  isNum,
  SESSION_ID,
  type Account,
  type AccountKey,
  type AppConfig,
  type Category,
  type Credit,
  type EventType,
  type Offer,
  type ProfileResponse,
  type RankedOffer,
  type RecommendCtx,
} from '@/lib/api';
import {
  ACCOUNT_META,
  ACTION_TEXT,
  CAT,
  CATEGORIES,
  CREDIT_OPTS,
  GOAL_OPTS,
  INCOME_RANGE,
  NAV,
  PAGES,
  SPEND_RANGE,
  cardName,
  eligibilityLabel,
  money,
  prefillFromAccount,
  signalsFor,
  tenureText,
  termsFor,
  tierLabel,
  type Signal,
} from '@/lib/catalog';

type Page = 'offermatch' | Category;

interface Answers {
  goal: Category | null;
  credit: Credit;
  income: number;
  spend: number;
}

interface ProductInputs {
  loanAmount: number;
  loanTerm: number;
  savingsDeposit: number;
  homePrice: number;
  downPaymentPct: number;
  investmentAmount: number;
  investmentMonthly: number;
}

interface RankState {
  offers: RankedOffer[];
  latency_ms: number | null;
  endpoint: string;
  ctx: RecommendCtx;
}

interface TraceLine {
  t: string;
  text: string;
  err?: boolean;
}

const DEFAULT_ANSWERS: Answers = { goal: null, credit: 'good', income: 60000, spend: 1500 };
const DEFAULT_PRODUCT_INPUTS: ProductInputs = {
  loanAmount: 15000,
  loanTerm: 36,
  savingsDeposit: 10000,
  homePrice: 500000,
  downPaymentPct: 20,
  investmentAmount: 5000,
  investmentMonthly: 250,
};
// Website clicks count toward ctx_session_cat_views for this long (matches the 10-minute training window).
const SESSION_WINDOW_MS = 10 * 60_000;
const APR: Record<Credit, number> = { excellent: 0.0899, good: 0.1149, fair: 0.1799 };

const now = () => new Date().toTimeString().slice(0, 8);
const pct = (s: number) => Math.round(s * 100);
const fmtMs = (v: unknown) => (isNum(v) ? `${Math.round(v)} ms` : '— ms');
type Click = { cat: Category; t: number };
const sessionCounts = (clicks: Click[]): Partial<Record<Category, number>> => {
  const cut = Date.now() - SESSION_WINDOW_MS;
  const out: Partial<Record<Category, number>> = {};
  for (const c of clicks) if (c.t >= cut) out[c.cat] = (out[c.cat] ?? 0) + 1;
  return out;
};
const toCtx = (
  a: Answers,
  clicks: Click[],
  activePage: Page,
  inputs: ProductInputs,
): RecommendCtx => ({
  // A product page is an explicit request for recommendations in that category. The click count
  // remains a separate request-time behavioral feature used by the model within that category.
  goal: activePage === 'offermatch' ? (a.goal ?? 'none') : activePage,
  credit: a.credit,
  income: a.income,
  card_spend: a.spend,
  ...(activePage === 'credit_card' ? { credit_monthly_spend: a.spend } : {}),
  ...(activePage === 'savings' ? { savings_deposit: inputs.savingsDeposit } : {}),
  ...(activePage === 'personal_loan'
    ? { loan_amount: inputs.loanAmount, loan_term_months: inputs.loanTerm }
    : {}),
  ...(activePage === 'mortgage'
    ? { home_price: inputs.homePrice, mortgage_down_payment_pct: inputs.downPaymentPct }
    : {}),
  ...(activePage === 'investment'
    ? { investment_amount: inputs.investmentAmount, investment_monthly_contribution: inputs.investmentMonthly }
    : {}),
  session_cat_views: sessionCounts(clicks),
});

function CardArt({ o, className = '' }: { o: Offer; className?: string }) {
  const c = CAT[o.product_category];
  return (
    <div className={`cardart ${className}`} style={{ background: c?.color }}>
      <span className="cat">{c?.label}</span>
      <span className="chipgfx" />
      <span className="nm">{cardName(o.offer_text)}</span>
    </div>
  );
}

function Chips({ signals, n }: { signals: Signal[]; n: number }) {
  return (
    <>
      {signals.slice(0, n).map((s) => (
        <span key={s.label} className={`chip ${s.kind}`}>
          {s.label}
        </span>
      ))}
    </>
  );
}

function ProductControls({
  category,
  credit,
  cardSpend,
  inputs,
  onCardSpend,
  onInput,
}: {
  category: Category;
  credit: Credit;
  cardSpend: number;
  inputs: ProductInputs;
  onCardSpend: (value: number) => void;
  onInput: (key: keyof ProductInputs, value: number, category: Category) => void;
}) {
  const shell = (title: string, body: ReactNode) => (
    <div className="calc xr">
      <span className="xr-label">preferences -&gt; request-time features</span>
      <h3>{title}</h3>
      {body}
    </div>
  );
  if (category === 'credit_card') {
    const fit = cardSpend >= 4000 ? 'Premium rewards' : cardSpend >= 1000 ? 'Everyday cashback' : 'Starter card';
    return shell('Tune your card match', <>
      <label>Monthly card spend <b>{money(cardSpend)}</b></label>
      <input type="range" min={0} max={8000} step={250} value={cardSpend} onChange={(e) => onCardSpend(+e.target.value)} />
      <div className="out"><div>Best-fit card family</div><b>{fit}</b></div>
    </>);
  }
  if (category === 'savings') {
    const fit = inputs.savingsDeposit >= 50000 ? 'Premier cash' : inputs.savingsDeposit >= 5000 ? 'High yield' : 'Everyday savings';
    return shell('Find the right place for your cash', <>
      <label>Opening deposit <b>{money(inputs.savingsDeposit)}</b></label>
      <input type="range" min={0} max={250000} step={5000} value={inputs.savingsDeposit} onChange={(e) => onInput('savingsDeposit', +e.target.value, category)} />
      <div className="out"><div>Best-fit account family</div><b>{fit}</b></div>
    </>);
  }
  if (category === 'personal_loan') {
    const r = APR[credit] / 12;
    const payment = (inputs.loanAmount * r) / (1 - Math.pow(1 + r, -inputs.loanTerm));
    return shell('Estimate your monthly payment', <>
      <label>Amount <b>{money(inputs.loanAmount)}</b></label>
      <input type="range" min={2000} max={50000} step={500} value={inputs.loanAmount} onChange={(e) => onInput('loanAmount', +e.target.value, category)} />
      <label>Term <b>{inputs.loanTerm} months</b></label>
      <input type="range" min={12} max={60} step={12} value={inputs.loanTerm} onChange={(e) => onInput('loanTerm', +e.target.value, category)} />
      <div className="out"><div>Est. monthly at {(APR[credit] * 100).toFixed(2)}% APR</div><b>{money(payment)}</b></div>
    </>);
  }
  if (category === 'mortgage') {
    const loan = inputs.homePrice * (1 - inputs.downPaymentPct / 100);
    return shell('Shape your home financing', <>
      <label>Home price <b>{money(inputs.homePrice)}</b></label>
      <input type="range" min={150000} max={2000000} step={25000} value={inputs.homePrice} onChange={(e) => onInput('homePrice', +e.target.value, category)} />
      <label>Down payment <b>{inputs.downPaymentPct}%</b></label>
      <input type="range" min={3} max={40} step={1} value={inputs.downPaymentPct} onChange={(e) => onInput('downPaymentPct', +e.target.value, category)} />
      <div className="out"><div>Estimated loan amount</div><b>{money(loan)}</b></div>
    </>);
  }
  const fit = inputs.investmentAmount >= 250000 ? 'Private advisory' : inputs.investmentAmount >= 25000 || inputs.investmentMonthly >= 1000 ? 'Managed growth' : 'Guided investing';
  return shell('Build your investing plan', <>
    <label>Starting amount <b>{money(inputs.investmentAmount)}</b></label>
    <input type="range" min={0} max={500000} step={5000} value={inputs.investmentAmount} onChange={(e) => onInput('investmentAmount', +e.target.value, category)} />
    <label>Monthly contribution <b>{money(inputs.investmentMonthly)}</b></label>
    <input type="range" min={0} max={5000} step={100} value={inputs.investmentMonthly} onChange={(e) => onInput('investmentMonthly', +e.target.value, category)} />
    <div className="out"><div>Best-fit investing path</div><b>{fit}</b></div>
  </>);
}

export function BankView(_props: { config: AppConfig | null }) {
  // ── Data from the server ─────────────────────────────────────
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [accountsErr, setAccountsErr] = useState<string | null>(null);
  const [catalog, setCatalog] = useState<Offer[]>([]);
  const [rank, setRank] = useState<RankState | null>(null);
  const [profile, setProfile] = useState<ProfileResponse | null>(null);
  const [err, setErr] = useState<string | null>(null);

  // ── Visitor state ────────────────────────────────────────────
  const [acctKey, setAcctKey] = useState<AccountKey | null>(null);
  const [answers, setAnswers] = useState<Answers>(DEFAULT_ANSWERS);
  const [searched, setSearched] = useState(false);
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState<Page>('offermatch');
  const [filter, setFilter] = useState<'all' | Category>('all');
  const [modal, setModal] = useState(false);
  const [productInputs, setProductInputs] = useState<ProductInputs>(DEFAULT_PRODUCT_INPUTS);
  const [actionModal, setActionModal] = useState<{
    mode: 'learn' | 'apply';
    offer: Offer;
    submitted?: boolean;
  } | null>(null);

  // ── Animation state ──────────────────────────────────────────
  const [moved, setMoved] = useState<Set<string>>(new Set());
  const [swap, setSwap] = useState(0);
  const [pulse, setPulse] = useState(false);
  const [toast, setToast] = useState<{ o: Offer; reason: string } | null>(null);
  const [toastOn, setToastOn] = useState(false);
  const [trace, setTrace] = useState<TraceLine[]>([]);

  const prevOrder = useRef<string[]>([]);
  const clicks = useRef<Click[]>([]); // this visit's website clicks (request-time feature)
  const prevTop = useRef<string | null>(null);
  const seq = useRef(0);
  const timers = useRef<Record<string, ReturnType<typeof setTimeout>>>({});

  const acct = accounts.find((a) => a.key === acctKey) ?? null;
  const meta = acct ? ACCOUNT_META[acct.key] : null;
  const customerId = acct?.customer_id ?? null;

  // Latest values for async callbacks (events wait ~1.2 s before re-ranking).
  const cur = useRef({ customerId, answers, page, productInputs });
  cur.current = { customerId, answers, page, productInputs };

  const later = (key: string, ms: number, fn: () => void) => {
    clearTimeout(timers.current[key]);
    timers.current[key] = setTimeout(fn, ms);
  };
  useEffect(() => () => Object.values(timers.current).forEach(clearTimeout), []);

  const log = useCallback((text: string, isErr = false) => {
    setTrace((t) => [...t.slice(-30), { t: now(), text, err: isErr }]);
  }, []);

  const who = (cid: string | null) => cid ?? `guest:${SESSION_ID.slice(0, 8)}`;

  // Rank and profile are independent endpoint calls. Do not make the offer cards wait for the
  // Feature Serving endpoint: render the ranking as soon as it arrives, then fill in profile signals.
  const refresh = useCallback(
    async (
      reason: string | null,
      eventCat?: Category,
      override?: {
        customerId: string | null;
        answers: Answers;
        page?: Page;
        productInputs?: ProductInputs;
      },
    ) => {
      const current = cur.current;
      const cid = override ? override.customerId : current.customerId;
      const ans = override?.answers ?? current.answers;
      const activePage = override?.page ?? current.page;
      const inputs = override?.productInputs ?? current.productInputs;
      const ctx = toCtx(ans, clicks.current, activePage, inputs);
      const my = ++seq.current;
      void api
        .profile(cid)
        .then((p) => {
          if (my !== seq.current) return;
          setProfile(p);
          const mob = eventCat ? p.categories?.[eventCat]?.cust_mobile_cat_views_10m : undefined;
          log(
            `  profile ${fmtMs(p.latency_ms)}` +
              (eventCat ? ` · mobile_cat_views_10m=${isNum(mob) ? mob : '—'} (stream)` : ''),
          );
        })
        .catch((e: Error) => my === seq.current && log(`profile        failed: ${e.message}`, true));

      let r: Awaited<ReturnType<typeof api.recommend>>;
      try {
        r = await api.recommend(cid, ctx);
      } catch (e) {
        if (my !== seq.current) return;
        const msg = (e as Error)?.message ?? String(e);
        setErr(msg);
        log(`rank           failed: ${msg}`, true);
        return;
      }
      if (my !== seq.current) return; // a newer refresh superseded this one
      setErr(null);
      const offers = [...(r.offers ?? [])].sort((a, b) => b.score - a.score);
      const order = offers.map((o) => o.offer_id);
      const prev = prevOrder.current;
      const top = offers[0];
      // Several offers share the same product copy; only a different product counts as a new top match.
      const topChanged = !!(top && prevTop.current && prevTop.current !== top.offer_text);

      if (prev.length) {
        const m = new Set(order.filter((id, i) => prev.indexOf(id) !== i));
        setMoved(m);
        later('moved', 900, () => setMoved(new Set()));
      }
      if (topChanged) {
        setSwap((s) => s + 1);
        setPulse(true);
        later('pulse', 1400, () => setPulse(false));
        if (reason) {
          setToast({ o: top, reason });
          setToastOn(true);
          later('toast', 3200, () => setToastOn(false));
        }
      }
      prevOrder.current = order;
      prevTop.current = top?.offer_text ?? null;
      setRank({ offers, latency_ms: isNum(r.latency_ms) ? r.latency_ms : null, endpoint: r.endpoint, ctx });
      log(`  re-ranked ${offers.length} offers ${fmtMs(r.latency_ms)}`);
    },
    [log],
  );

  // ── Boot: accounts, offer catalog, and a first guest ranking for the For-you pill + rail ──
  useEffect(() => {
    api
      .accounts()
      .then((d) => setAccounts((d.accounts ?? []).filter((a) => a.key in ACCOUNT_META)))
      .catch((e: Error) => setAccountsErr(e.message));
    api
      .offers()
      .then((d) => setCatalog(d.offers ?? []))
      .catch(() => setCatalog([]));
    refresh(null);
  }, [refresh]);

  // A visitor interaction: the click is a request-time feature (ctx_session_cat_views), so the very next
  // ranking request already reflects it. No round trip through a stream.
  const emit = useCallback(
    async (type: EventType, cat: Category, label: string, pageOverride?: Page) => {
      clicks.current = [...clicks.current, { cat, t: Date.now() }];
      const n = sessionCounts(clicks.current)[cat] ?? 0;
      log(`${type.padEnd(14)} ${cat.padEnd(13)} ${who(cur.current.customerId)} -> ctx_session_cat_views=${n} (request)`);
      await refresh(
        `${ACTION_TEXT[type] ?? 'viewed'} ${label}`,
        cat,
        pageOverride ? { ...cur.current, page: pageOverride } : undefined,
      );
    },
    [log, refresh],
  );

  const go = (p: Page) => {
    setPage(p);
    window.scrollTo(0, 0);
    if (p !== 'offermatch') emit('page_view', p, `${CAT[p].label.toLowerCase()} page`, p);
  };
  const apply = (o: Offer) => {
    setActionModal({ mode: 'apply', offer: o });
    void emit('add_to_cart', o.product_category, cardName(o.offer_text));
  };
  const learnMore = (o: Offer) => {
    setActionModal({ mode: 'learn', offer: o });
    void emit('product_view', o.product_category, cardName(o.offer_text), page === 'offermatch' ? undefined : page);
  };

  const signIn = (a: Account) => {
    const pre = prefillFromAccount(a);
    const next: Answers = { goal: null, credit: pre.credit, income: pre.income, spend: pre.card_spend };
    setAcctKey(a.key);
    setAnswers(next);
    setSearched(true);
    setPage('offermatch');
    setFilter('all');
    setModal(false);
    prevOrder.current = [];
    prevTop.current = null;
    clicks.current = [];
    setTrace([{ t: now(), text: `sign_in        ${a.customer_id} -> lookup tier, tenure, balances` }]);
    setLoading(true);
    refresh(null, undefined, { customerId: a.customer_id, answers: next, page: 'offermatch' }).finally(() => setLoading(false));
  };
  const asGuest = () => {
    setAcctKey(null);
    setModal(false);
    prevTop.current = null;
    clicks.current = [];
    log('guest          customer_id=null -> server derives guest id from session_id');
    refresh(null, undefined, { customerId: null, answers });
  };

  const find = () => {
    const first = !searched;
    setSearched(true);
    setPage('offermatch');
    log(
      `offermatch     goal=${answers.goal ?? 'none'} credit=${answers.credit} income=${answers.income} spend=${answers.spend} -> request features`,
    );
    setLoading(first);
    refresh(first ? null : 'updated your answers').finally(() => setLoading(false));
  };

  // Product controls update immediately, then debounce one real ranking request while the visitor drags.
  const emitRef = useRef(emit);
  emitRef.current = emit;
  const scheduleProductRank = (category: Category, label: string) =>
    later(`control-${category}`, 300, () => {
      const type: EventType = category === 'personal_loan' ? 'calculator_use' : 'preference_change';
      void emitRef.current(type, category, label, category);
    });
  const changeProductInput = (key: keyof ProductInputs, value: number, category: Category) => {
    const next = { ...cur.current.productInputs, [key]: value };
    cur.current.productInputs = next;
    setProductInputs(next);
    const label = category === 'savings'
      ? `${money(next.savingsDeposit)} opening deposit`
      : category === 'personal_loan'
        ? `${money(next.loanAmount)} over ${next.loanTerm} months`
        : category === 'mortgage'
          ? `${money(next.homePrice)} home with ${next.downPaymentPct}% down`
          : `${money(next.investmentAmount)} to start and ${money(next.investmentMonthly)} monthly`;
    scheduleProductRank(category, label);
  };
  const changeCardSpend = (value: number) => {
    const next = { ...cur.current.answers, spend: value };
    cur.current.answers = next;
    setAnswers(next);
    scheduleProductRank('credit_card', `${money(value)} monthly card spend`);
  };

  // ── Derived ──────────────────────────────────────────────────
  const ranked = rank?.offers ?? [];
  const top = ranked[0] ?? null;
  const orderIdx = useMemo(() => new Map(ranked.map((o, i) => [o.offer_id, i])), [ranked]);
  const sig = (o: Offer) => signalsFor(o, rank?.ctx ?? null, profile, !!acct);

  // ── Header ───────────────────────────────────────────────────
  const header = (
    <div className="bank-top">
      <div className="bank-nav xr">
        <span className="xr-label">Every click -&gt; request-time feature (this visit)</span>
        <div className="logo" onClick={() => go('offermatch')}>
          <i />
          Lakeshore Bank
        </div>
        {NAV.map((n) => (
          <a key={n.key} className={page === n.key ? 'on' : ''} onClick={() => go(n.key)}>
            {n.label}
          </a>
        ))}
        <a className={`om-link${page === 'offermatch' ? ' on' : ''}`} onClick={() => go('offermatch')}>
          OfferMatch
        </a>
        <div className="nav-right">
          <div className={`foryou${pulse ? ' pulse' : ''}`} title="Your top offer" onClick={() => go('offermatch')}>
            <i style={top ? { background: CAT[top.product_category]?.color } : undefined} />
            <span>{top ? `For you: ${cardName(top.offer_text)}` : 'Your top offer'}</span>
          </div>
          {acct && meta ? (
            <div className="acct xr" onClick={() => setModal(true)}>
              <span className="xr-label">customer_id -&gt; online feature lookup</span>
              <div className="avatar" style={{ background: meta.color }}>
                {meta.first[0]}
              </div>
              <div>
                {meta.first}
                <small>{tierLabel(acct.loyalty_tier)} member</small>
              </div>
            </div>
          ) : (
            <button className="signin-btn" onClick={() => setModal(true)}>
              Sign in
            </button>
          )}
        </div>
      </div>
    </div>
  );

  // ── OfferMatch ───────────────────────────────────────────────
  const sidebar = (
    <aside className="om-side xr">
      <span className="xr-label">Answers -&gt; request-time features (RequestSource)</span>
      <h2>OfferMatch</h2>
      <div className="lede">Answer a few questions and we'll match you with offers. No impact to your credit score.</div>
      {meta ? (
        <div className="prefill">Prefilled from your Lakeshore profile, {meta.first}. Change anything that's out of date.</div>
      ) : (
        <div className="prefill">
          Signed-in customers get better matches. <a onClick={() => setModal(true)}>Sign in</a>
        </div>
      )}
      <div className="q">
        <label>What are you looking for?</label>
        <div className="opts">
          {GOAL_OPTS.map((g) => (
            <button
              key={g.v}
              className={`opt${answers.goal === g.v ? ' on' : ''}`}
              onClick={() => setAnswers((a) => ({ ...a, goal: a.goal === g.v ? null : g.v }))}
            >
              {g.label}
            </button>
          ))}
        </div>
      </div>
      <div className="q">
          <label>How would you rate your credit?</label>
          <div className="opts">
            {CREDIT_OPTS.map((c) => (
              <button
                key={c.v}
                className={`opt${answers.credit === c.v ? ' on' : ''}`}
                onClick={() => setAnswers((a) => ({ ...a, credit: c.v }))}
              >
                {c.label}
              </button>
            ))}
          </div>
      </div>
      <div className="q">
          <label>
            Annual income <b>{money(answers.income)}</b>
          </label>
          <input
            type="range"
            min={INCOME_RANGE.min}
            max={INCOME_RANGE.max}
            step={INCOME_RANGE.step}
            value={answers.income}
            onChange={(e) => setAnswers((a) => ({ ...a, income: +e.target.value }))}
          />
      </div>
      <div className="q">
          <label>
            Monthly card spend <b>{money(answers.spend)}</b>
          </label>
          <input
            type="range"
            min={SPEND_RANGE.min}
            max={SPEND_RANGE.max}
            step={SPEND_RANGE.step}
            value={answers.spend}
            onChange={(e) => setAnswers((a) => ({ ...a, spend: +e.target.value }))}
          />
      </div>
      <button className="btn dark cta" onClick={find}>
        {searched ? 'Update my offers' : 'Find my offers'}
      </button>
      <div className="fine">Credit, income, and spend all influence product suitability. Matches also update as you browse.</div>
    </aside>
  );

  let results: ReactNode;
  if (!searched) {
    results = (
      <div className="om-empty">
        <h1>Offers picked for you, not for everyone</h1>
        <p>
          Tell us what you're after. We'll rank every Lakeshore card, account, loan, and investing product for you, and
          keep the list fresh as you browse.
        </p>
        <div className="how3">
          <div>
            <b>1</b>Answer a few questions
          </div>
          <div>
            <b>2</b>See your ranked matches
          </div>
          <div>
            <b>3</b>Matches update as you explore
          </div>
        </div>
      </div>
    );
  } else if (loading || (!top && !err)) {
    results = (
      <div className="loading">
        <div className="spinner" />
        Matching you with offers...
      </div>
    );
  } else if (!top) {
    results = <div className="errbox">We couldn't rank offers right now: {err}</div>;
  } else {
    const selectedGoal = rank?.ctx.goal !== 'none' ? rank?.ctx.goal : null;
    const selectedLabel = selectedGoal ? CAT[selectedGoal].label : null;
    const topSig = sig(top);
    const others = ranked
      .slice(1)
      .filter((o) => filter === 'all' || o.product_category === filter)
      .slice(0, 8);
    results = (
      <>
        {err && <div className="errbox">Last update failed: {err}. Showing your previous matches.</div>}
        <div className="res-h">
          <div>
            <h2>
              {selectedLabel
                ? `Your top ${selectedLabel.toLowerCase()} matches${meta ? `, ${meta.first}` : ''}`
                : meta
                  ? `Your top matches, ${meta.first}`
                  : 'Your top matches'}
            </h2>
            <p>
              {ranked.length} offers ranked for you in {rank?.latency_ms != null ? `${Math.round(rank.latency_ms)} ms` : '— ms'}
            </p>
          </div>
          <span className="live">Updating as you browse</span>
        </div>
        <div key={`top-${swap}`} className={`topm xr${swap ? ' swap' : ''}`}>
          <span className="xr-label">Model Serving: rank, {fmtMs(rank?.latency_ms)}</span>
          <span className="ribbon">{selectedLabel ? `TOP ${selectedLabel.toUpperCase()} MATCH` : 'YOUR TOP MATCH'}</span>
          <CardArt o={top} />
          <div>
            <div className="tag">
              {CAT[top.product_category]?.label} · {eligibilityLabel(top.tier_requirement)}
            </div>
            <h3>
              {cardName(top.offer_text)} <span className="oid">{top.offer_id}</span>
            </h3>
            <div className="tag">{top.offer_text}</div>
            <div className="terms">
              {termsFor(top).map(([b, l]) => (
                <div key={l}>
                  <b>{b}</b>
                  {l}
                </div>
              ))}
            </div>
            <div className="why">
              <div>Signals behind this match</div>
              {topSig.length ? <Chips signals={topSig} n={4} /> : <span className="none">No strong signals yet. Answer the questions or browse the site.</span>}
            </div>
            <div style={{ display: 'flex', gap: 8, marginTop: 12 }}>
              <button className="btn primary" onClick={() => apply(top)}>
                Apply now
              </button>
              <button className="btn ghost" onClick={() => learnMore(top)}>
                See details
              </button>
            </div>
          </div>
          <div className="match">
            <div className="ring" style={{ background: `conic-gradient(var(--bankAccent) ${top.score * 360}deg,#eef0f3 0)` }}>
              <span>{pct(top.score)}%</span>
            </div>
            <small>match</small>
          </div>
        </div>
        {!selectedGoal && (
          <div className="ftabs" style={{ marginTop: 20 }}>
            {([['all', 'All'], ...CATEGORIES.map((k) => [k, CAT[k].label])] as [string, string][]).map(([k, l]) => (
              <button key={k} className={filter === k ? 'on' : ''} onClick={() => setFilter(k as 'all' | Category)}>
                {l}
              </button>
            ))}
          </div>
        )}
        <div className="grid-m">
          {others.map((o) => (
            <div key={o.offer_id} className={`mcard${moved.has(o.offer_id) ? ' moved' : ''}`}>
              <CardArt o={o} />
              <div>
                <div className="rk">
                  #{(orderIdx.get(o.offer_id) ?? 0) + 1} · {CAT[o.product_category]?.label} · {o.offer_id}
                </div>
                <h4>{cardName(o.offer_text)}</h4>
                <div className="bar">
                  <i style={{ width: `${pct(o.score)}%`, background: CAT[o.product_category]?.solid }} />
                </div>
                <div style={{ marginTop: 7 }}>
                  <Chips signals={sig(o)} n={1} />
                </div>
                <div className="row">
                  <button className="btn primary" style={{ padding: '6px 11px' }} onClick={() => apply(o)}>
                    Apply
                  </button>
                  <button className="btn ghost" style={{ padding: '6px 11px' }} onClick={() => learnMore(o)}>
                    Details
                  </button>
                </div>
              </div>
              <span className="pct">{pct(o.score)}%</span>
            </div>
          ))}
        </div>
        <div className="res-foot">
          <span>Match % is the model's estimated likelihood you'd accept, given your profile, answers, and this visit.</span>
          <span>Rates subject to approval.</span>
        </div>
      </>
    );
  }

  // ── Product page ─────────────────────────────────────────────
  let productPage: ReactNode = null;
  if (page !== 'offermatch') {
    const P = PAGES[page];
    const source: Offer[] = catalog.length ? catalog : ranked;
    const scoreOf = new Map(ranked.map((o) => [o.offer_id, o.score]));
    const list = source
      .filter((o) => o.product_category === page)
      .sort((a, b) => (orderIdx.get(a.offer_id) ?? 1e9) - (orderIdx.get(b.offer_id) ?? 1e9));
    productPage = (
      <>
        <div className="hero">
          <div className="hero-in">
            <div>
              <div className="crumb">{P.crumb}</div>
              <h1>{P.title}</h1>
              <p>{P.sub}</p>
              <div className="stats">
                {P.stats.map(([b, l]) => (
                  <div key={l}>
                    <b>{b}</b>
                    {l}
                  </div>
                ))}
              </div>
            </div>
            <ProductControls
              category={page}
              credit={answers.credit}
              cardSpend={answers.spend}
              inputs={productInputs}
              onCardSpend={changeCardSpend}
              onInput={changeProductInput}
            />
          </div>
        </div>
        <div className="bank-body">
          <div>
            <div className="sec-h">
              <h2>{P.list}</h2>
              <span>Editorial ratings, updated daily</span>
            </div>
            {list.length === 0 && <div className="rail-empty">Loading products...</div>}
            {list.map((o) => {
              const s = scoreOf.get(o.offer_id);
              return (
                <div key={o.offer_id} className="product">
                  {top && o.offer_id === top.offer_id && <span className="ribbon2">YOUR TOP MATCH</span>}
                  <div className="art" style={{ background: CAT[o.product_category]?.color }}>
                    {cardName(o.offer_text)}
                  </div>
                  <div>
                    <h4>
                      {cardName(o.offer_text)} <span className="oid">{o.offer_id}</span>
                    </h4>
                    <div className="meta">
                      {termsFor(o).map(([b, l]) => (
                        <div key={l}>
                          <b>{b}</b>
                          {l}
                        </div>
                      ))}
                      <div>
                        <b>{s != null ? `${pct(s)}%` : '—'}</b>your match
                      </div>
                      <div>
                        <b>{eligibilityLabel(o.tier_requirement)}</b>eligibility
                      </div>
                    </div>
                  </div>
                  <div className="actions">
                    <button className="btn primary" onClick={() => apply(o)}>
                      Apply now
                    </button>
                    <button className="btn ghost" onClick={() => learnMore(o)}>
                      Learn more
                    </button>
                  </div>
                </div>
              );
            })}
          </div>
          <aside className="rail xr">
            <span className="xr-label">Model Serving: rank all offers</span>
            <div className="rail-h">
              <b>Recommended for you</b>
              <small>{meta ? `For ${meta.first}, updated as you browse` : 'Updated as you browse'}</small>
            </div>
            {ranked.length === 0 && <div className="rail-empty">{err ? `Ranking unavailable: ${err}` : 'Ranking offers...'}</div>}
            {ranked.length > 0 && err && <div className="rail-empty">Last update failed: {err}. Showing previous matches.</div>}
            {ranked.slice(0, 4).map((o) => (
              <div key={o.offer_id} className={`rail-row${moved.has(o.offer_id) ? ' moved' : ''}`}>
                <div className="mini" style={{ background: CAT[o.product_category]?.color }} />
                <div>
                  <b>{cardName(o.offer_text)}</b>
                  <span>
                    {pct(o.score)}% match · {sig(o)[0]?.label ?? CAT[o.product_category]?.label}
                  </span>
                </div>
              </div>
            ))}
            <div className="rail-foot">
              <a onClick={() => go('offermatch')}>See all your matches</a>
            </div>
          </aside>
        </div>
      </>
    );
  }

  const traceLines = trace.slice(-8).reverse();

  return (
    <section>
      {header}
      {page === 'offermatch' ? (
        <div className="om">
          {sidebar}
          <main className="om-main">{results}</main>
        </div>
      ) : (
        productPage
      )}

      {modal && (
        <div className="modal-bg" onClick={(e) => e.target === e.currentTarget && setModal(false)}>
          <div className="modal">
            <h3>Welcome back</h3>
            <p>Choose an account to sign in. Signed-in matches use your Lakeshore profile and history.</p>
            {accountsErr && <div className="errbox">Accounts unavailable: {accountsErr}</div>}
            {!accountsErr && accounts.length === 0 && <div className="rail-empty">Loading accounts...</div>}
            {accounts.map((a) => {
              const m = ACCOUNT_META[a.key];
              return (
                <button key={a.key} className="acct-opt" onClick={() => signIn(a)}>
                  <div className="avatar" style={{ background: m.color }}>
                    {m.first[0]}
                  </div>
                  <div>
                    <b>{m.name}</b>
                    <small>
                      {m.sub} · {tenureText(a.tenure_months)}
                    </small>
                  </div>
                  <span className="tierb">{tierLabel(a.loyalty_tier)}</span>
                </button>
              );
            })}
            <button className="guest" onClick={asGuest}>
              Continue as guest
            </button>
          </div>
        </div>
      )}

      {actionModal && (
        <div className="modal-bg" onClick={(e) => e.target === e.currentTarget && setActionModal(null)}>
          <div className="modal action-modal" role="dialog" aria-modal="true" aria-label={`${actionModal.mode} ${cardName(actionModal.offer.offer_text)}`}>
            <button className="modal-x" aria-label="Close" onClick={() => setActionModal(null)}>×</button>
            <div className="action-kicker">{CAT[actionModal.offer.product_category].label} · {actionModal.offer.offer_id}</div>
            {actionModal.mode === 'learn' ? (
              <>
                <h3>{cardName(actionModal.offer.offer_text)}</h3>
                <p>{actionModal.offer.offer_text}</p>
                <div className="detail-terms">
                  {termsFor(actionModal.offer).map(([value, label]) => (
                    <div key={label}><b>{value}</b><span>{label}</span></div>
                  ))}
                </div>
                <div className="detail-why">
                  <b>Why this may fit you</b>
                  <div><Chips signals={sig(actionModal.offer)} n={4} /></div>
                </div>
                <div className="demo-note">Demo product details · rates and eligibility are illustrative.</div>
                <div className="modal-actions">
                  <button className="btn ghost" onClick={() => setActionModal(null)}>Close</button>
                  <button className="btn primary" onClick={() => apply(actionModal.offer)}>Apply now</button>
                </div>
              </>
            ) : actionModal.submitted ? (
              <>
                <div className="success-mark">✓</div>
                <h3>Application started</h3>
                <p>Your demo application for {cardName(actionModal.offer.offer_text)} is ready for the next step.</p>
                <div className="reference">Reference <b>DEMO-{actionModal.offer.offer_id.replace(/\D/g, '').padStart(4, '0')}</b></div>
                <div className="demo-note">Demo only · no application was submitted and no credit check was performed.</div>
                <div className="modal-actions">
                  <button className="btn primary" onClick={() => setActionModal(null)}>Done</button>
                </div>
              </>
            ) : (
              <>
                <h3>Start your application</h3>
                <p>{cardName(actionModal.offer.offer_text)}</p>
                <div className="apply-steps">
                  <div className="on"><b>1</b><span>Your details</span></div>
                  <i />
                  <div><b>2</b><span>Review</span></div>
                  <i />
                  <div><b>3</b><span>Decision</span></div>
                </div>
                <div className="application-summary">
                  <div><span>Applicant</span><b>{meta?.name ?? 'Guest visitor'}</b></div>
                  <div><span>Product</span><b>{cardName(actionModal.offer.offer_text)}</b></div>
                  <div><span>Credit impact</span><b>None to continue</b></div>
                </div>
                <div className="demo-note">Demo application flow · the next button does not submit personal data.</div>
                <div className="modal-actions">
                  <button className="btn ghost" onClick={() => setActionModal(null)}>Cancel</button>
                  <button className="btn primary" onClick={() => setActionModal({ ...actionModal, submitted: true })}>Continue application</button>
                </div>
              </>
            )}
          </div>
        </div>
      )}

      <div className={`toast${toastOn ? ' on' : ''}`}>
        <i style={{ background: toast ? CAT[toast.o.product_category]?.color : undefined }} />
        <div>
          <span>{toast ? `New top match: ${cardName(toast.o.offer_text)}` : ''}</span>
          <small>{toast ? `Because you ${toast.reason}` : ''}</small>
        </div>
      </div>

      <div className="trace">
        <div className="th">
          DATABRICKS LAYER · live request trace
        </div>
        {traceLines.length ? (
          traceLines.map((l, i) => (
            <div key={`${l.t}-${trace.length - i}`} className={l.err ? 'err' : i === 0 ? 'new' : ''}>
              {l.t} {l.text}
            </div>
          ))
        ) : (
          <div>Interact with the site to see events flow.</div>
        )}
      </div>
    </section>
  );
}
