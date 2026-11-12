<script setup lang="ts">
import { computed } from 'vue';
import { marketTag } from '@/utils/format';

const props = defineProps<{ code: string; category: string; market?: string }>();

const label = computed(() => {
  if (props.market === 'HK') return '港股';
  if (props.market === 'US') return '美股';
  return marketTag(props.code, props.category);
});

const cls = computed(() => {
  if (props.market === 'HK' || props.market === 'US') return 'tag-foreign';
  if (props.category === 'ZS') return 'tag-index';
  if (props.category === 'ETF' || props.category === 'LOF') return 'tag-fund';
  return 'tag-stock';
});
</script>

<template>
  <span v-if="label" class="market-tag" :class="cls">{{ label }}</span>
</template>

<style scoped>
.market-tag {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  /* 固定最小宽度，让「沪指/深A/ETF」等不同字数标签等宽，代码列对齐 */
  min-width: 36px;
  height: 18px;
  padding: 0 6px;
  font-size: var(--text-xs);
  line-height: 1;
  border-radius: var(--radius-sm);
  white-space: nowrap;
  font-family: var(--font-sans);
  font-weight: var(--font-weight-medium);
}
.tag-stock {
  color: var(--color-accent);
  background: var(--color-accent-dim);
}
.tag-index {
  color: var(--color-warning);
  background: var(--color-warning-bg);
}
.tag-fund {
  color: var(--color-text-secondary);
  background: var(--color-bg-elevated);
}
.tag-foreign {
  color: var(--color-info, #4da3ff);
  background: var(--color-info-bg, rgba(77, 163, 255, 0.12));
}
</style>
