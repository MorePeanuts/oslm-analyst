"""
OSLM-Analyst Dashboard - Streamlit UI for visualizing OSIR-LMTS data.
"""

from pathlib import Path
from datetime import datetime
import pandas as pd
import streamlit as st


def get_available_months(output_root: Path) -> list[str]:
    """Get list of available months from output directory."""
    months = []
    if not output_root.exists():
        return months
    for dir_path in output_root.iterdir():
        if dir_path.is_dir() and dir_path.name.startswith('osir-lmts_'):
            month = dir_path.name.split('_')[1]
            months.append(month)
    return sorted(months, reverse=True)


def load_csv(data_dir: Path, filename: str) -> pd.DataFrame | None:
    """Load CSV file if it exists."""
    file_path = data_dir / filename
    if file_path.exists():
        return pd.read_csv(file_path)
    return None


def render_overall_rank(df: pd.DataFrame | None, title: str = 'Overall Ranking'):
    """Render overall ranking table."""
    st.subheader(title)
    if df is None or df.empty:
        st.info('No data available')
        return

    display_df = df.copy()
    if 'org' in display_df.columns:
        display_df = display_df.set_index('org')

    # Format numeric columns
    for col in display_df.columns:
        if col not in ['rank', 'delta_rank']:
            display_df[col] = display_df[col].round(4)

    st.dataframe(
        display_df.sort_values('rank'),
        use_container_width=True,
    )


def render_summary_table(df: pd.DataFrame | None, title: str):
    """Render summary table."""
    st.subheader(title)
    if df is None or df.empty:
        st.info('No data available')
        return

    if 'org' in df.columns:
        df = df.set_index('org')

    st.dataframe(df, use_container_width=True)


def render_trend_chart(all_data: dict[str, pd.DataFrame], metric: str, org: str | None = None):
    """Render trend chart for a specific metric over time."""
    if not all_data:
        return

    records = []
    for month, df in sorted(all_data.items()):
        if df is None:
            continue
        if org:
            row = df[df['org'] == org]
            if not row.empty:
                val = row.iloc[0].get(metric)
                if pd.notna(val):
                    records.append({'month': month, metric: val})
        else:
            # Take top 1 orgs by rank
            if 'rank' in df.columns:
                top_row = df.sort_values('rank').iloc[0]
                val = top_row.get(metric)
                if pd.notna(val):
                    records.append({'month': month, metric: val})

    if records:
        trend_df = pd.DataFrame(records).set_index('month')
        st.line_chart(trend_df)


def main():
    """Main Streamlit app entry point."""
    st.set_page_config(
        page_title='OSLM-Analyst Dashboard',
        page_icon='📊',
        layout='wide',
    )

    st.title('📊 OSLM-Analyst Dashboard')
    st.markdown('Open-source large models data analyst - OSIR-LMTS')

    # Get project root (assuming we're in src/oslm_analyst/ui/)
    project_root = Path(__file__).parents[3]
    output_root = project_root / 'output'

    # Sidebar configuration
    st.sidebar.header('Configuration')

    available_months = get_available_months(output_root)
    if not available_months:
        st.error(f'No data found in {output_root}. Please run the osir-lmts processor first.')
        return

    selected_month = st.sidebar.selectbox(
        'Select Month',
        available_months,
        index=0,
    )

    show_accumulated = st.sidebar.checkbox('Show Accumulated Data', value=False)
    show_domestic = st.sidebar.checkbox('Show Domestic (CN) Only', value=False)

    # Load data for selected month
    data_dir = output_root / f'osir-lmts_{selected_month}'

    # Load all ranking files
    overall_rank = load_csv(data_dir, 'overall_rank.csv')
    model_rank = load_csv(data_dir, 'model_rank.csv')
    dataset_rank = load_csv(data_dir, 'dataset_rank.csv')
    infra_rank = load_csv(data_dir, 'infra_rank.csv')
    eval_rank = load_csv(data_dir, 'eval_rank.csv')

    acc_overall_rank = load_csv(data_dir, 'acc_overall_rank.csv')
    acc_model_rank = load_csv(data_dir, 'acc_model_rank.csv')
    acc_dataset_rank = load_csv(data_dir, 'acc_dataset_rank.csv')

    cn_overall_rank = load_csv(data_dir, 'CN_overall_rank.csv')
    cn_acc_overall_rank = load_csv(data_dir, 'CN_acc_overall_rank.csv')

    # Load summary files
    model_summary = load_csv(data_dir, 'model_summary.csv')
    dataset_summary = load_csv(data_dir, 'dataset_summary.csv')
    infra_summary = load_csv(data_dir, 'infra_summary.csv')
    eval_summary = load_csv(data_dir, 'eval_summary.csv')

    acc_model_summary = load_csv(data_dir, 'acc_model_summary.csv')
    acc_dataset_summary = load_csv(data_dir, 'acc_dataset_summary.csv')

    delta_model_summary = load_csv(data_dir, 'delta_model_summary.csv')
    delta_dataset_summary = load_csv(data_dir, 'delta_dataset_summary.csv')

    # Tabs
    tab_overall, tab_model, tab_dataset, tab_infra, tab_eval, tab_trends = st.tabs(
        [
            '🏆 Overall',
            '🤖 Model',
            '📚 Dataset',
            '🏗️ Infra',
            '📝 Eval',
            '📈 Trends',
        ]
    )

    with tab_overall:
        st.header('Overall Rankings')

        if show_domestic:
            if show_accumulated:
                render_overall_rank(cn_acc_overall_rank, 'CN Accumulated Overall Ranking')
            else:
                render_overall_rank(cn_overall_rank, 'CN Overall Ranking')
        else:
            if show_accumulated:
                render_overall_rank(acc_overall_rank, 'Accumulated Overall Ranking')
            else:
                render_overall_rank(overall_rank, 'Overall Ranking')

        if overall_rank is not None and not overall_rank.empty:
            st.subheader('Score Breakdown')
            top_orgs = overall_rank.sort_values('rank').head(5)['org'].tolist()
            selected_org = st.selectbox('Select Organization for Details', top_orgs, index=0)

            if selected_org:
                org_data = overall_rank[overall_rank['org'] == selected_org]
                if not org_data.empty:
                    cols = st.columns(4)
                    org_row = org_data.iloc[0]
                    cols[0].metric('Model Influence', f'{org_row.get("model_influence", 0):.4f}')
                    cols[1].metric(
                        'Dataset Influence', f'{org_row.get("dataset_influence", 0):.4f}'
                    )
                    cols[2].metric('Infra Influence', f'{org_row.get("infra_influence", 0):.4f}')
                    cols[3].metric('Eval Influence', f'{org_row.get("eval_influence", 0):.4f}')

    with tab_model:
        st.header('Model Dimension')

        if show_accumulated:
            render_overall_rank(acc_model_rank, 'Accumulated Model Ranking')
            render_summary_table(acc_model_summary, 'Accumulated Model Summary')
        else:
            render_overall_rank(model_rank, 'Model Ranking')
            render_summary_table(model_summary, 'Model Summary')
            render_summary_table(delta_model_summary, 'Model Delta (Change from Previous Month)')

    with tab_dataset:
        st.header('Dataset Dimension')

        if show_accumulated:
            render_overall_rank(acc_dataset_rank, 'Accumulated Dataset Ranking')
            render_summary_table(acc_dataset_summary, 'Accumulated Dataset Summary')
        else:
            render_overall_rank(dataset_rank, 'Dataset Ranking')
            render_summary_table(dataset_summary, 'Dataset Summary')
            render_summary_table(
                delta_dataset_summary, 'Dataset Delta (Change from Previous Month)'
            )

    with tab_infra:
        st.header('Infra Dimension')
        render_overall_rank(infra_rank, 'Infra Ranking')
        render_summary_table(infra_summary, 'Infra Summary')

    with tab_eval:
        st.header('Eval Dimension')
        render_overall_rank(eval_rank, 'Eval Ranking')
        render_summary_table(eval_summary, 'Eval Summary')

    with tab_trends:
        st.header('Trends Over Time')

        # Load all months data for trends
        all_overall_rank = {}
        for month in get_available_months(output_root):
            month_dir = output_root / f'osir-lmts_{month}'
            df = load_csv(month_dir, 'overall_rank.csv')
            if df is not None:
                all_overall_rank[month] = df

        if all_overall_rank:
            # Get unique orgs across all months
            all_orgs = set()
            for df in all_overall_rank.values():
                if 'org' in df.columns:
                    all_orgs.update(df['org'].tolist())

            selected_org = st.selectbox(
                'Select Organization (optional)',
                ['(All - Top)'] + sorted(list(all_orgs)),
                index=0,
            )

            org_param = selected_org if selected_org != '(All - Top)' else None

            col1, col2 = st.columns(2)
            with col1:
                st.subheader('Score Trend')
                render_trend_chart(all_overall_rank, 'score', org_param)
            with col2:
                st.subheader('Rank Trend')
                render_trend_chart(all_overall_rank, 'rank', org_param)

    # Footer
    st.markdown('---')
    st.caption(
        f'Data source: {output_root} | Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
    )


if __name__ == '__main__':
    main()

