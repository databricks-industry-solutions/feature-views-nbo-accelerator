// The real code behind each How it works step, cut from the accelerator's own files at build time, so the
// snippets on screen never drift from the notebooks. Each excerpt is located by a literal anchor line.
import type { AppConfig } from '@/lib/api';
import nb01 from '../../../../../notebooks/part1_feature_views/01_generate_data.py?raw';
import nb02 from '../../../../../notebooks/part1_feature_views/02_define_feature_views.py?raw';
import nb03 from '../../../../../notebooks/part1_feature_views/03_materialize_features.py?raw';
import nb04 from '../../../../../notebooks/part1_feature_views/04_train_ranker.py?raw';
import nb07 from '../../../../../notebooks/part2_realtime_two_stage/07_streaming_feature_views.py?raw';
import nb09 from '../../../../../notebooks/part2_realtime_two_stage/09_realtime_serving.py?raw';
import nb10 from '../../../../../notebooks/part2_realtime_two_stage/10_feature_serving_endpoint.py?raw';
import streamTs from '../../../server/stream.ts?raw';
import serverTs from '../../../server/server.ts?raw';

const SOURCES: Record<string, string> = {
  'notebooks/part1_feature_views/01_generate_data.py': nb01,
  'notebooks/part1_feature_views/02_define_feature_views.py': nb02,
  'notebooks/part1_feature_views/03_materialize_features.py': nb03,
  'notebooks/part1_feature_views/04_train_ranker.py': nb04,
  'notebooks/part2_realtime_two_stage/07_streaming_feature_views.py': nb07,
  'notebooks/part2_realtime_two_stage/09_realtime_serving.py': nb09,
  'notebooks/part2_realtime_two_stage/10_feature_serving_endpoint.py': nb10,
  'apps/recommender-ui/server/stream.ts': streamTs,
  'apps/recommender-ui/server/server.ts': serverTs,
};

interface Range {
  from: string; // literal text of the first line (first match in the file)
  lines: number;
}
interface Spec {
  id: string;
  step: number; // index into the How it works steps
  label: string;
  file: keyof typeof SOURCES;
  ranges: Range[];
}
export interface Snippet extends Omit<Spec, 'ranges'> {
  line: number; // 1-based line of the first range
  html: string;
}

const P01 = 'notebooks/part1_feature_views/01_generate_data.py';
const P02 = 'notebooks/part1_feature_views/02_define_feature_views.py';
const P03 = 'notebooks/part1_feature_views/03_materialize_features.py';
const P04 = 'notebooks/part1_feature_views/04_train_ranker.py';
const P07 = 'notebooks/part2_realtime_two_stage/07_streaming_feature_views.py';
const P09 = 'notebooks/part2_realtime_two_stage/09_realtime_serving.py';
const P10 = 'notebooks/part2_realtime_two_stage/10_feature_serving_endpoint.py';
const STREAM = 'apps/recommender-ui/server/stream.ts';
const SERVER = 'apps/recommender-ui/server/server.ts';

const SPECS: Spec[] = [
  { id: 'batch-define', step: 0, label: 'Sliding + Tumbling', file: P02, ranges: [{ from: 'avg_balance_30d = get_or_create_feature(', lines: 14 }] },
  { id: 'batch-materialize', step: 0, label: 'Materialize', file: P03, ranges: [{ from: 'fe.materialize_features(', lines: 6 }] },
  { id: 'source-data', step: 0, label: 'Source data', file: P01, ranges: [{ from: 'CREATE OR REPLACE TABLE customers AS', lines: 10 }] },
  { id: 'stream-rolling', step: 1, label: 'Rolling window', file: P07, ranges: [
    { from: 'stream_source = StreamSource(full_name=STREAM_NAME)', lines: 1 },
    { from: 'clicks_10m = fe.create_feature(', lines: 7 },
  ] },
  { id: 'stream-sawtooth', step: 1, label: 'Sawtooth (Beta)', file: P07, ranges: [{ from: 'cat_views_30d = fe.create_feature(', lines: 7 }] },
  { id: 'stream-materialize', step: 1, label: 'StreamingMode', file: P07, ranges: [{ from: 'fe.materialize_features(', lines: 9 }] },
  { id: 'stream-produce', step: 1, label: 'Kafka producer', file: STREAM, ranges: [
    { from: 'function clickEvent(customerId: string): ClickEvent {', lines: 12 },
    { from: 'async function produceKafka(', lines: 8 },
  ] },
  { id: 'train-set', step: 2, label: 'Training set', file: P04, ranges: [{ from: 'training_set = fe.create_training_set(', lines: 6 }] },
  { id: 'train-log', step: 2, label: 'Log with lineage', file: P04, ranges: [{ from: 'fe.log_model(model=model, artifact_path="ranker"', lines: 3 }] },
  { id: 'serve-request', step: 3, label: 'App request', file: SERVER, ranges: [{ from: 'const records = allOffers.map((o) => ({', lines: 6 }] },
  { id: 'serve-ranker', step: 3, label: 'Ranker endpoint', file: P09, ranges: [{ from: 'served = [ServedEntityInput(name=SERVED', lines: 8 }] },
  { id: 'serve-udf', step: 3, label: 'CustomUDF', file: P10, ranges: [{ from: 'spend_to_income = fe.create_feature(', lines: 7 }] },
  { id: 'serve-fse', step: 3, label: 'Feature Serving', file: P10, ranges: [
    { from: 'fe.create_feature_spec(name=SPEC', lines: 1 },
    { from: 'served = [ServedEntityInput(entity_name=SPEC', lines: 6 },
  ] },
  { id: 'observe-timing', step: 4, label: 'Event timing', file: STREAM, ranges: [{ from: 'if (value !== undefined && value >= target.expect) {', lines: 9 }] },
];

/** The lines of each range, dedented, joined with an ellipsis line. Throws if an anchor is missing. */
export function extract(src: string, ranges: Range[]): { text: string; line: number } {
  const lines = src.split('\n');
  let first = 0;
  const parts = ranges.map((r, i) => {
    const at = lines.findIndex((l) => l.includes(r.from));
    if (at < 0) throw new Error(`code excerpt anchor not found: ${r.from}`);
    if (i === 0) first = at + 1;
    const block = lines.slice(at, at + r.lines);
    const indent = Math.min(...block.filter((l) => l.trim()).map((l) => l.length - l.trimStart().length));
    return block.map((l) => l.slice(indent)).join('\n');
  });
  return { text: parts.join('\n...\n'), line: first };
}

const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const TOKENS = {
  py: /(#[^\n]*)|(f?"""[\s\S]*?"""|f?"(?:[^"\\\n]|\\.)*"|f?'(?:[^'\\\n]|\\.)*')|\b(def|class|import|from|try|except|if|else|elif|for|in|return|not|and|or|None|True|False|with|as|lambda|raise)\b/g,
  ts: /(\/\/[^\n]*)|("(?:[^"\\\n]|\\.)*"|'(?:[^'\\\n]|\\.)*'|`(?:[^`\\]|\\.)*`)|\b(const|let|async|await|function|return|if|else|new|for|of|throw|export|interface|type)\b/g,
};

/** Minimal highlighting: comments, strings, keywords. Everything else is escaped text. */
export function highlight(code: string, lang: 'py' | 'ts'): string {
  const re = new RegExp(TOKENS[lang]);
  let out = '';
  let last = 0;
  for (let m = re.exec(code); m; m = re.exec(code)) {
    out += esc(code.slice(last, m.index));
    out += `<span class="${m[1] ? 'c' : m[2] ? 's' : 'k'}">${esc(m[0])}</span>`;
    last = m.index + m[0].length;
  }
  return out + esc(code.slice(last));
}

/**
 * One block per source line, highlighted on its own, so a line too wide for the box wraps with a hanging
 * indent (4 characters past its own indent) instead of restarting at column 0.
 */
export function renderLines(code: string, lang: 'py' | 'ts'): string {
  return code.split('\n').map((l) => {
    const indent = l.length - l.trimStart().length;
    return `<span class="ln" style="--i:${indent}ch">${highlight(l.trimStart(), lang) || ' '}</span>`;
  }).join('');
}

export const SNIPPETS: Snippet[] = SPECS.map(({ ranges, ...s }) => {
  const { text, line } = extract(SOURCES[s.file], ranges);
  return { ...s, line, html: renderLines(text, s.file.endsWith('.py') ? 'py' : 'ts') };
});

/** Workspace URL for a bundle file (notebooks open without their .py extension), or '' when unknown. */
export function workspaceUrl(config: AppConfig | null, file = ''): string {
  if (!config?.workspace_host || !config.files_path) return '';
  const root = config.files_path.replace(/^\/Workspace(?=\/)/, '');
  const rel = file.startsWith('notebooks/') ? file.replace(/\.py$/, '') : file;
  return `${config.workspace_host}/#workspace${root}${rel ? `/${rel}` : ''}`;
}
