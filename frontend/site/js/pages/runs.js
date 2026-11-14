import { api } from '../api.js';
import { can, ENUMS, reasonTone, replayTone, runDisplay, runStatusTone } from '../config.js';
import {
  bindFilterBar,
  dataTable,
  drawer,
  errorBox,
  errorMessage,
  esc,
  field,
  filterBar,
  fmtTime,
  fmtTimeShort,
  kv,
  metricGrid,
  pct,
  toast,
} from '../ui.js';

const state = { tableId: '', status: '' };

// 处理中的批次自动轮询刷新（页面切走即停），让“处理中 / 剩余数量”实时前进
let pollTimer = null;
function stopPolling() {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}

function statusBadge(row) {
  return `<span class="badge ${runStatusTone(row.status)}" data-testid="run-status">${esc(
    ENUMS.runStatus[row.status] || row.status,
  )}</span>`;
}

function progressCell(row) {
  const progress = row.progress || {};
  const percent = Math.round((progress.percent || 0) * 100);
  const total = progress.total_events || row.events_in || 0;
  const done = progress.processed_events || 0;
  const remaining = progress.remaining_events ?? Math.max(0, total - done);
  const mode = row.status === 'interrupted' ? 'interrupted' : row.display_status === 'completed' ? 'done' : '';
  const meta =
    row.display_status === 'processing'
      ? `剩 ${remaining}`
      : row.review_pending > 0
        ? `待复核 ${row.review_pending}`
        : '完成';
  return `<div class="run-progress ${mode}">
    <span class="rp-track"><span class="rp-fill" style="width:${percent}%"></span></span>
    <span class="rp-meta pill-count">${esc(meta)}</span>
  </div>`;
}

function rejectTable(rejects, editable) {
  if (!rejects.length) return '<p class="muted">这个批次没有拒绝行。</p>';
  return `<div class="table-wrap"><table class="data" data-testid="reject-list" style="min-width:0">
    <thead><tr><th>主键</th><th>原因</th><th>LSN</th><th>事件时间</th><th>处置</th>${editable ? '<th>操作</th>' : ''}</tr></thead>
    <tbody>${rejects
      .map(
        (row) => `<tr data-reject="${row.id}">
          <td class="mono">${esc(row.pk_value)}</td>
          <td><span class="badge ${reasonTone(row.reason)}">${esc(ENUMS.rejectReason[row.reason] || row.reason)}</span></td>
          <td class="mono">${esc(row.event_lsn)}</td>
          <td class="mono">${esc(fmtTimeShort(row.event_time))}</td>
          <td><span class="badge ${replayTone(row.replay_status)}">${esc(row.replay_status_label)}</span></td>
          ${
            editable
              ? `<td class="row-actions">
                  <button class="btn sm ghost" data-act="replayed" data-id="${row.id}">重放</button>
                  <button class="btn sm ghost" data-act="skipped" data-id="${row.id}">跳过</button>
                </td>`
              : ''
          }
        </tr>`,
      )
      .join('')}</tbody></table></div>`;
}

function perTableRows(metrics) {
  const tables = Object.values(metrics.tables || {});
  if (!tables.length) return '<p class="muted">没有按表拆分的指标。</p>';
  return `<div class="table-wrap"><table class="data" style="min-width:0">
    <thead><tr><th>表</th><th>事件</th><th>upsert</th><th>删除</th><th>迟到</th><th>重复</th><th>拒绝</th><th>确认时间</th></tr></thead>
    <tbody>${tables
      .map(
        (table) => `<tr>
          <td class="mono">${esc(table.table_name)}</td>
          <td class="num">${esc(table.events)}</td>
          <td class="num">${esc(table.upsert_rows)}</td>
          <td class="num">${esc(table.deletes)}</td>
          <td class="num">${esc(table.late_count)}</td>
          <td class="num">${esc(table.dup_skipped)}</td>
          <td class="num">${esc(table.rejected)}</td>
          <td class="mono">${esc(fmtTimeShort(table.confirmed_at))}</td>
        </tr>`,
      )
      .join('')}</tbody></table></div>`;
}

function sampleRows(samples) {
  if (!samples || !samples.length) return '<p class="muted">没有可展示的终态样例。</p>';
  return `<div class="table-wrap"><table class="data" data-testid="sample-rows" style="min-width:0">
    <thead><tr><th>表</th><th>主键</th><th>终态字段</th><th>最后事件时间</th><th>LSN</th></tr></thead>
    <tbody>${samples
      .map(
        (row) => `<tr>
          <td class="mono">${esc(row.table)}</td>
          <td class="mono">${esc(row.pk_value)}</td>
          <td class="mono">${esc(Object.entries(row.after || {}).slice(0, 3).map(([k, v]) => `${k}=${v}`).join(' · '))}</td>
          <td class="mono">${esc(fmtTimeShort(row.last_event_time))}</td>
          <td class="mono">${esc(row.last_lsn)}</td>
        </tr>`,
      )
      .join('')}</tbody></table></div>`;
}

function reconciliationPanel(recon) {
  if (!recon) return '';
  const checks = (recon.checks || []).map(
    (check) => `<li>
      <span class="${check.ok ? 'ri-ok' : 'ri-bad'}">${check.ok ? '✓' : '✗'}</span>
      <span class="mono">${esc(check.detail)}</span>
    </li>`,
  );
  return `<h3 class="drawer-h3">完成对账（源事件 / 应用结果 / 待复核）</h3>
  <div class="metric-grid">
    <div class="metric"><span class="k">源事件数</span><b class="pill-count">${esc(recon.source_events)}</b><span class="hint">快照边界内全部事件</span></div>
    <div class="metric ok"><span class="k">应用结果</span><b class="pill-count">${esc(recon.applied_events)}</b><span class="hint">折叠应用的事件</span></div>
    <div class="metric alert"><span class="k">待复核记录</span><b class="pill-count">${esc(recon.review_pending)}</b><span class="hint">已重放 ${esc(recon.review_replayed)} · 已跳过 ${esc(recon.review_skipped)}</span></div>
  </div>
  <ul class="recon-list">${checks.join('')}</ul>
  <p class="tiny muted ${recon.balanced ? '' : 'danger-text'}">对账结论：${recon.balanced ? '三方对平，批次成功' : '存在未对平项，批次标记为部分成功（待复核）'}</p>`;
}

function resumePanel(detail) {
  const run = detail.run;
  if (!run.resumable) return '';
  const progress = run.progress || {};
  return `<div class="resume-banner" data-testid="resume-banner">
    <div class="rb-text">
      批次在 <b>${esc(ENUMS.runStatus[run.status] || run.status)}</b>：已确认 ${esc(progress.confirmed_tables || 0)}/${esc(
        progress.total_tables || 0,
      )} 张表，
      已处理 ${esc(progress.processed_events || 0)}/${esc(progress.total_events || 0)}，剩余 ${esc(
        progress.remaining_events ?? 0,
      )} 条。续跑将从最后确认位置接上，已确认结果不重算、拒绝行不重复产出。
    </div>
    ${
      can(detail._role, 'run:create')
        ? '<button class="btn accent" id="resume-run">断点续跑</button>'
        : '<span class="tiny muted">仅管理员可续跑</span>'
    }
  </div>`;
}

async function openRun(view, ctx, run) {
  const editable = can(ctx.user.role, 'reject:replay');
  let detail;
  try {
    detail = await api.get(`/api/runs/${run.id}/detail`);
  } catch (err) {
    toast(errorMessage(err), 'err');
    return;
  }
  detail._role = ctx.user.role;
  const metrics = detail.run.metrics || {};
  const display = runDisplay(detail.run);
  const { root, close } = drawer({
    wide: true,
    title: `合并批次 ${detail.run.run_no}`,
    subtitle: `${detail.run.scope} · ${ENUMS.trigger[detail.run.trigger] || detail.run.trigger} · ${detail.run.operator_email}`,
    body: `
      ${resumePanel(detail)}
      ${metricGrid([
        { label: '页面状态', value: display.label, hint: ENUMS.runStatus[detail.run.status] || detail.run.status, tone: display.tone },
        { label: '读取事件', value: detail.run.events_in, hint: '批次范围 ' + detail.run.scope },
        { label: '剩余 / 待复核', value: `${detail.run.progress?.remaining_events ?? 0} / ${detail.run.review_pending}`, hint: '剩余处理量 / 待复核拒绝行' },
        { label: 'upsert 行', value: detail.run.upsert_rows, hint: '按 (pk) 折叠后的终态', tone: 'ok' },
        { label: '删除终态', value: detail.run.deleted_rows, hint: '最后一次操作是 d' },
        { label: '迟到事件', value: detail.run.late_count, hint: `迟到率 ${pct(detail.run.late_ratio)}`, tone: 'warn' },
        { label: '重复去重', value: detail.run.dup_skipped, hint: '同 (pk, lsn) 只计首次' },
        { label: '拒绝行', value: detail.run.rejected_count, hint: '重复 / 旧更新 / 类型穿帮', tone: 'alert' },
      ])}
      ${kv([
        ['水位线', `<span class="mono">${fmtTimeShort(detail.run.watermark_at)}</span>`],
        ['快照边界', `<span class="mono">事件 id ≤ ${esc(detail.run.snapshot_max_event_id ?? '—')}</span>`],
        ['允许迟到', `${esc(detail.run.allowed_lateness_seconds)} 秒`],
        ['执行次数', `${esc(detail.run.attempt_count)} 次（中断续跑 ${Math.max(0, (detail.run.attempt_count || 1) - 1)} 次）`],
        ['最近心跳', `<span class="mono">${fmtTimeShort(detail.run.heartbeat_at)}</span>`],
        ['开始 / 结束', `${fmtTime(detail.run.started_at)} → ${fmtTime(detail.run.finished_at)}`],
      ])}
      <h3 class="drawer-h3">断点进度（确认点）</h3>
      ${progressCell(detail.run)}
      <h3 class="drawer-h3">按表拆分（算法 A 逐表折叠，表级确认后续跑跳过）</h3>
      ${perTableRows(metrics)}
      ${reconciliationPanel(detail.run.reconciliation)}
      <h3 class="drawer-h3">折叠终态样例（按 event_time + lsn 排序后谁赢）</h3>
      ${sampleRows(metrics.samples)}
      <h3 class="drawer-h3">运行日志</h3>
      <pre class="code">${esc(detail.run.log_text || '（无日志）')}</pre>
      <h3 class="drawer-h3">拒绝行（${esc(detail.rejects.total)} 条）</h3>
      ${rejectTable(detail.rejects.items, editable)}`,
    footer: can(ctx.user.role, 'export:read')
      ? `<div class="drawer-actions">
          <button class="btn ghost" data-export="merge-runs">导出批次 CSV</button>
          <button class="btn ghost" data-export="rejects">导出拒绝行 CSV</button>
        </div>`
      : '<span class="mono muted">只读角色不能导出</span>',
  });

  root.querySelector('#resume-run')?.addEventListener('click', async () => {
    const button = root.querySelector('#resume-run');
    button.disabled = true;
    button.textContent = '续跑中…';
    try {
      const resumed = await api.post(`/api/runs/${detail.run.id}/resume`, {});
      toast(`${resumed.run_no} 续跑完成：${resumed.display_status_label}，待复核 ${resumed.review_pending} 条`);
      close();
      await openRun(view, ctx, resumed);
      await runsPage.render(view, ctx);
    } catch (err) {
      toast(errorMessage(err), 'err');
      button.disabled = false;
      button.textContent = '断点续跑';
    }
  });

  root.querySelectorAll('[data-act]').forEach((button) => {
    button.addEventListener('click', async () => {
      try {
        await api.patch(`/api/rejects/${button.dataset.id}/replay`, {
          replay_status: button.dataset.act,
          note: `批次 ${detail.run.run_no} 手动处置`,
        });
        toast(button.dataset.act === 'replayed' ? '已标记重放' : '已跳过该拒绝行');
        close();
        await openRun(view, ctx, detail.run);
      } catch (err) {
        toast(errorMessage(err), 'err');
      }
    });
  });
  root.querySelector('[data-export="merge-runs"]')?.addEventListener('click', async () => {
    try {
      await api.download('/api/exports/merge-runs.csv', 'merge-runs.csv');
      toast('已导出合并批次 CSV');
    } catch (err) {
      toast(errorMessage(err), 'err');
    }
  });
  root.querySelector('[data-export="rejects"]')?.addEventListener('click', async () => {
    try {
      await api.download(`/api/exports/rejects.csv?run_id=${detail.run.id}`, 'rejects.csv');
      toast('已导出拒绝行 CSV');
    } catch (err) {
      toast(errorMessage(err), 'err');
    }
  });
}

function openRunForm(view, ctx, tables) {
  const { root, close } = drawer({
    title: '发起合并批次',
    subtitle: '服务端按水位线重新折叠事件，分块确认、可中断续跑',
    body: `<form id="run-form" class="form-grid">
      ${field({
        label: '批次范围',
        name: 'table_id',
        options: [{ value: '', label: '全库（3 张表一起折叠）' }, ...tables.map((table) => ({ value: table.id, label: `${table.code} · ${table.name}` }))],
      })}
      ${field({ label: '允许迟到（秒，空则用表配置）', name: 'allowed_lateness_seconds', type: 'number', value: '30', attrs: 'min="0" max="3600"' })}
      ${field({ label: '触发方式', name: 'trigger', value: 'manual', options: [{ value: 'manual', label: '手动' }, { value: 'schedule', label: '定时补偿' }] })}
      <div class="form-error" id="run-error"></div>
      <button class="btn accent" type="submit">开始合并</button>
    </form>`,
  });
  root.querySelector('#run-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    const form = event.target;
    const button = form.querySelector('button[type=submit]');
    button.disabled = true;
    button.textContent = '合并中…';
    try {
      const run = await api.post('/api/runs', {
        table_id: form.table_id.value ? Number(form.table_id.value) : null,
        allowed_lateness_seconds: form.allowed_lateness_seconds.value
          ? Number(form.allowed_lateness_seconds.value)
          : null,
        trigger: form.trigger.value,
      });
      toast(`${run.run_no} 完成：${run.display_status_label}，upsert ${run.upsert_rows} / 待复核 ${run.review_pending}`);
      close();
      await runsPage.render(view, ctx);
    } catch (err) {
      root.querySelector('#run-error').textContent = errorMessage(err);
      button.disabled = false;
      button.textContent = '开始合并';
    }
  });
}

export const runsPage = {
  async render(view, ctx) {
    stopPolling();
    let runs;
    let tables;
    try {
      [runs, tables] = await Promise.all([
        api.get('/api/runs', { limit: 30, table_id: state.tableId, status: state.status }),
        api.get('/api/tables', { limit: 50 }),
      ]);
    } catch (err) {
      view.innerHTML = errorBox(errorMessage(err));
      view.querySelector('#retry-btn')?.addEventListener('click', () => runsPage.render(view, ctx));
      return;
    }
    const hasProcessing = runs.items.some((row) => row.display_status === 'processing');
    view.innerHTML = `
      <section class="panel">
        <div class="panel-head">
          <h2>合并批次与拒绝行</h2>
          <div class="head-actions">
            <span class="meta">${runs.total} 个批次</span>
            ${can(ctx.user.role, 'run:create') ? '<button class="btn sm accent" id="new-run">发起合并</button>' : ''}
            ${can(ctx.user.role, 'export:read') ? '<button class="btn sm ghost" id="export-runs">导出批次 CSV</button>' : ''}
          </div>
        </div>
        <div class="panel-body">
          ${filterBar({
            id: 'run-filters',
            fields: [
              {
                label: '范围',
                name: 'table_id',
                value: state.tableId,
                options: [{ value: '', label: '全部' }, ...tables.items.map((table) => ({ value: table.id, label: table.code }))],
              },
              {
                label: '状态',
                name: 'status',
                value: state.status,
                options: [{ value: '', label: '全部' }, ...Object.entries(ENUMS.runStatus).map(([value, label]) => ({ value, label }))],
              },
            ],
          })}
        </div>
        ${dataTable({
          rows: runs.items,
          testId: 'run-list',
          empty: { title: '还没有合并批次', hint: '管理员可以发起一次全库合并。' },
          columns: [
            { key: 'run_no', label: '批次号', className: 'mono' },
            { key: 'scope', label: '范围' },
            { key: 'status', label: '状态', render: (row) => statusBadge(row) },
            {
              key: 'progress',
              label: '处理进度 / 剩余',
              render: (row) => progressCell(row),
            },
            { key: 'events_in', label: '事件', className: 'num' },
            { key: 'review_pending', label: '待复核', className: 'num', render: (row) => (row.review_pending ? `<span class="danger-text mono pill-count">${esc(row.review_pending)}</span>` : '0') },
            { key: 'late_count', label: '迟到', className: 'num', render: (row) => (row.late_count ? `<span class="warn-text mono">${row.late_count}</span>` : '0') },
            { key: 'dup_skipped', label: '重复', className: 'num' },
            {
              key: 'actions',
              label: '操作',
              render: (row) =>
                row.resumable && can(ctx.user.role, 'run:create')
                  ? '<span class="run-row-actions"><button class="btn sm accent" data-resume="' + row.id + '">续跑</button></span>'
                  : '<span class="muted tiny">—</span>',
            },
            { key: 'started_at', label: '开始时间', render: (row) => fmtTime(row.started_at) },
          ],
        })}
      </section>`;

    bindFilterBar(view, 'run-filters', (values) => {
      state.tableId = values.table_id;
      state.status = values.status;
      runsPage.render(view, ctx);
    });
    view.querySelectorAll('table.data tbody tr').forEach((row) => {
      row.addEventListener('click', (event) => {
        if (event.target.closest('[data-resume]')) return; // 续跑按钮不触发抽屉
        const item = runs.items.find((entry) => String(entry.id) === row.dataset.id);
        if (item) openRun(view, ctx, item);
      });
    });
    view.querySelector('#new-run')?.addEventListener('click', () => openRunForm(view, ctx, tables.items));
    view.querySelectorAll('[data-resume]').forEach((button) => {
      button.addEventListener('click', async (event) => {
        event.stopPropagation();
        const id = button.dataset.resume;
        button.disabled = true;
        button.textContent = '续跑中…';
        try {
          const run = await api.post(`/api/runs/${id}/resume`, {});
          toast(`${run.run_no} 续跑完成：${run.display_status_label}，待复核 ${run.review_pending} 条`);
          await runsPage.render(view, ctx);
        } catch (err) {
          toast(errorMessage(err), 'err');
          button.disabled = false;
          button.textContent = '续跑';
        }
      });
    });
    view.querySelector('#export-runs')?.addEventListener('click', async () => {
      try {
        await api.download('/api/exports/merge-runs.csv', 'merge-runs.csv');
        toast('已导出合并批次 CSV');
      } catch (err) {
        toast(errorMessage(err), 'err');
      }
    });

    // 处理中的批次：每 2.5 秒静默轮询，进度与剩余数量自动前进；中断后停止并提示续跑
    if (hasProcessing) {
      pollTimer = setInterval(async () => {
        try {
          const latest = await api.get('/api/runs', { limit: 30, table_id: state.tableId, status: state.status });
          const stillProcessing = latest.items.some((row) => row.display_status === 'processing');
          const anyInterrupted = latest.items.some((row) => row.status === 'interrupted');
          if (anyInterrupted) toast('检测到批次中断，可从确认点断点续跑', 'err');
          await runsPage.render(view, ctx);
          if (!stillProcessing) stopPolling();
        } catch {
          stopPolling();
        }
      }, 2500);
    }
  },
};

export default runsPage;
