import { describe, expect, it } from 'vitest';
import { extract, highlight, renderLines, SNIPPETS, workspaceUrl } from './code';

describe('How it works code excerpts', () => {
  it('finds every anchor in the real files, and every step has code', () => {
    // Importing the module already throws on a missing anchor; this pins the shape on top.
    for (const s of SNIPPETS) {
      expect(s.line, s.id).toBeGreaterThan(0);
      expect(s.html.length, s.id).toBeGreaterThan(40);
    }
    expect(new Set(SNIPPETS.map((s) => s.step))).toEqual(new Set([0, 1, 2, 3, 4]));
    expect(new Set(SNIPPETS.map((s) => s.id)).size).toBe(SNIPPETS.length);
  });

  it('shows the live code, not a paraphrase', () => {
    const text = (id: string) => SNIPPETS.find((s) => s.id === id)!.html.replace(/<[^>]+>/g, '');
    expect(text('stream-rolling')).toContain('RollingWindow(window_duration=timedelta(minutes=10))');
    expect(text('stream-sawtooth')).toContain('SawtoothWindow(window_duration=timedelta(days=30))');
    expect(text('train-set')).toContain('fe.create_training_set(');
    expect(text('serve-fse')).toContain('route_optimized=True');
    expect(text('stream-produce')).toContain('function clickEvent(');
  });

  it('extracts, dedents, and joins ranges', () => {
    const src = 'a\n    if x:\n        y = 1\n    z\nb';
    expect(extract(src, [{ from: 'if x:', lines: 2 }])).toEqual({ text: 'if x:\n    y = 1', line: 2 });
    expect(extract(src, [{ from: 'a', lines: 1 }, { from: 'b', lines: 1 }]).text).toBe('a\n...\nb');
    expect(() => extract(src, [{ from: 'missing', lines: 1 }])).toThrow(/anchor not found/);
  });

  it('escapes HTML and marks comments, strings, keywords', () => {
    expect(highlight('x = "<b>"  # note', 'py')).toBe('x = <span class="s">"&lt;b&gt;"</span>  <span class="c"># note</span>');
    expect(highlight('const a = `t`;', 'ts')).toBe('<span class="k">const</span> a = <span class="s">`t`</span>;');
  });

  it('renders one block per line with its indent, keeping blank lines', () => {
    expect(renderLines('a = 1\n\n    # c', 'py')).toBe(
      '<span class="ln" style="--i:0ch">a = 1</span>' +
      '<span class="ln" style="--i:0ch"> </span>' +
      '<span class="ln" style="--i:4ch"><span class="c"># c</span></span>',
    );
  });

  it('builds workspace links, dropping .py for notebooks', () => {
    const config = { catalog: 'c', schema: 's', ranker_endpoint: 'r', feature_endpoint: 'f', traffic_enabled: true,
      workspace_host: 'https://h', files_path: '/Workspace/Users/u/.bundle/b/dev/files' };
    expect(workspaceUrl(config, 'notebooks/part1/04_train_ranker.py')).toBe('https://h/#workspace/Users/u/.bundle/b/dev/files/notebooks/part1/04_train_ranker');
    expect(workspaceUrl(config, 'apps/x/server/stream.ts')).toBe('https://h/#workspace/Users/u/.bundle/b/dev/files/apps/x/server/stream.ts');
    expect(workspaceUrl(null, 'x')).toBe('');
  });
});
