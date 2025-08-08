#!/usr/bin/env python3
"""
KV Store Benchmark HTML Report Generator

This script generates beautiful HTML reports from JSON benchmark results.
It can process individual benchmark runs or compare multiple runs.

Usage:
    python3 generate_benchmark_report.py <results_directory>
    python3 generate_benchmark_report.py --compare run1 run2 run3
    python3 generate_benchmark_report.py --list
"""

import json
import os
import sys
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
import glob


class BenchmarkReportGenerator:
    """Generate HTML reports from benchmark JSON results"""
    
    def __init__(self):
        self.results: Dict[str, Any] = {}
        self.comparison_mode = False
        self.run_dirs: List[str] = []
        self.templates_dir = Path(__file__).parent / "templates"
    
    def load_single_run(self, results_dir: str, enable_averaging: bool = True) -> Dict[str, Any]:
        """Load all JSON result files from a single run directory"""
        results = {}
        results_path = Path(results_dir)
        
        if not results_path.exists():
            raise FileNotFoundError(f"Results directory '{results_dir}' not found")
        
        json_files = list(results_path.glob('*.json'))
        if not json_files:
            raise ValueError(f"No JSON files found in '{results_dir}'")
        
        # Group files by base name (without iteration suffix)
        file_groups = {}
        for json_file in json_files:
            # Check if this is an iteration file (e.g., "test_iter1.json")
            if '_iter' in json_file.stem and enable_averaging:
                base_name = json_file.stem.split('_iter')[0]
                if base_name not in file_groups:
                    file_groups[base_name] = []
                file_groups[base_name].append(json_file)
            else:
                # Regular file, load directly
                test_name = json_file.stem
                try:
                    with open(json_file, 'r') as f:
                        results[test_name] = json.load(f)
                except json.JSONDecodeError as e:
                    print(f"Warning: Failed to parse {json_file}: {e}")
                    continue
        
        # Process grouped files (multiple iterations)
        for base_name, iteration_files in file_groups.items():
            if len(iteration_files) > 1:
                # Average the iterations
                try:
                    averaged_result = self._average_benchmark_results(iteration_files)
                    results[base_name] = averaged_result
                    print(f"Averaged {len(iteration_files)} iterations for {base_name}")
                except Exception as e:
                    print(f"Warning: Failed to average iterations for {base_name}: {e}")
                    # Fallback to first iteration
                    try:
                        with open(iteration_files[0], 'r') as f:
                            results[base_name] = json.load(f)
                    except json.JSONDecodeError as e2:
                        print(f"Warning: Failed to parse fallback file {iteration_files[0]}: {e2}")
                        continue
            else:
                # Single iteration, load directly
                try:
                    with open(iteration_files[0], 'r') as f:
                        results[base_name] = json.load(f)
                except json.JSONDecodeError as e:
                    print(f"Warning: Failed to parse {iteration_files[0]}: {e}")
                    continue
        
        return results
    
    def load_multiple_runs(self, run_dirs: List[str]) -> Dict[str, Dict[str, Any]]:
        """Load results from multiple run directories for comparison"""
        all_runs = {}
        
        for run_dir in run_dirs:
            run_name = os.path.basename(run_dir)
            try:
                all_runs[run_name] = self.load_single_run(run_dir)
            except (FileNotFoundError, ValueError) as e:
                print(f"Warning: Skipping {run_dir}: {e}")
                continue
        
        if not all_runs:
            raise ValueError("No valid run directories found")
        
        return all_runs
    
    def _average_benchmark_results(self, input_files: List[Path]) -> Dict[str, Any]:
        """Average multiple benchmark result files into a single result"""
        if not input_files:
            raise ValueError("No input files provided")
        
        results = []
        
        # Load all result files
        for file_path in input_files:
            if not file_path.exists():
                raise FileNotFoundError(f"Input file not found: {file_path}")
            
            try:
                with open(file_path, 'r') as f:
                    result = json.load(f)
                    results.append(result)
            except json.JSONDecodeError as e:
                raise json.JSONDecodeError(f"Invalid JSON in file {file_path}: {e}", e.doc, e.pos)
        
        if not results:
            raise ValueError("No valid results loaded")
        
        # Use the first result as template
        avg_result = results[0].copy()
        
        # Add metadata about averaging
        avg_result['config']['iterations'] = len(results)
        avg_result['config']['averaged'] = True
        
        # Validate that all results have compatible structures
        first_stats_count = len(results[0].get('statistics', []))
        for i, result in enumerate(results[1:], 1):
            if len(result.get('statistics', [])) != first_stats_count:
                raise ValueError(f"Incompatible statistics structure in file {input_files[i]}")
        
        # Average the statistics
        for stat_idx, stat in enumerate(avg_result['statistics']):
            # Collect values from all iterations for this statistic
            values = {
                'count': [],
                'success': [],
                'failed': [],
                'min_latency': [],
                'max_latency': [],
                'avg_latency': [],
                'p50_latency': [],
                'p90_latency': [],
                'p95_latency': [],
                'p99_latency': [],
                'throughput': []
            }
            
            # Collect values from all result files
            for result in results:
                if stat_idx < len(result['statistics']):
                    s = result['statistics'][stat_idx]
                    
                    # Validate that all required fields exist
                    required_fields = ['count', 'success', 'failed', 'min_latency', 'max_latency', 
                                     'avg_latency', 'p50_latency', 'p90_latency', 'p95_latency', 
                                     'p99_latency', 'throughput']
                    
                    for field in required_fields:
                        if field not in s:
                            raise ValueError(f"Missing required field '{field}' in statistics")
                        values[field].append(s[field])
            
            # Calculate averages for each metric
            stat['count'] = int(sum(values['count']) / len(values['count']))
            stat['success'] = int(sum(values['success']) / len(values['success']))
            stat['failed'] = int(sum(values['failed']) / len(values['failed']))
            stat['min_latency'] = int(sum(values['min_latency']) / len(values['min_latency']))
            stat['max_latency'] = int(sum(values['max_latency']) / len(values['max_latency']))
            stat['avg_latency'] = int(sum(values['avg_latency']) / len(values['avg_latency']))
            stat['p50_latency'] = int(sum(values['p50_latency']) / len(values['p50_latency']))
            stat['p90_latency'] = int(sum(values['p90_latency']) / len(values['p90_latency']))
            stat['p95_latency'] = int(sum(values['p95_latency']) / len(values['p95_latency']))
            stat['p99_latency'] = int(sum(values['p99_latency']) / len(values['p99_latency']))
            stat['throughput'] = sum(values['throughput']) / len(values['throughput'])
        
        # Average the total duration
        total_durations = [r['total_duration'] for r in results]
        avg_result['total_duration'] = int(sum(total_durations) / len(total_durations))
        
        return avg_result
    
    def format_duration(self, nanoseconds: int) -> str:
        """Format duration from nanoseconds to human readable"""
        if nanoseconds < 1000:
            return f"{nanoseconds}ns"
        elif nanoseconds < 1000000:
            return f"{nanoseconds/1000:.1f}μs"
        elif nanoseconds < 1000000000:
            return f"{nanoseconds/1000000:.1f}ms"
        else:
            return f"{nanoseconds/1000000000:.2f}s"
    
    def format_number(self, number: float) -> str:
        """Format large numbers with commas"""
        return f"{number:,.0f}"
    
    def get_throughput_class(self, throughput: float) -> str:
        """Get CSS class for throughput color coding"""
        if throughput > 100000:
            return "metric-high"
        elif throughput > 10000:
            return "metric-medium"
        else:
            return "metric-low"
    
    def _load_template(self, template_name: str) -> str:
        """Load template content from templates directory"""
        template_path = self.templates_dir / template_name
        if not template_path.exists():
            raise FileNotFoundError(f"Template not found: {template_path}")
        
        with open(template_path, 'r') as f:
            return f.read()
    
    def generate_css(self) -> str:
        """Load CSS from template file"""
        return self._load_template('report.css')
    
    def generate_single_run_html(self, results: Dict[str, Any], output_dir: str) -> str:
        """Generate HTML report for a single benchmark run"""
        run_id = os.path.basename(output_dir)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Check if results are averaged
        sample_result = list(results.values())[0] if results else None
        is_averaged = False
        iterations = 1
        if sample_result:
            config = sample_result['config']
            is_averaged = config.get('averaged', False)
            iterations = config.get('iterations', 1)
        
        # Build subtitle with averaging info
        subtitle = f"Run ID: {run_id}"
        if is_averaged:
            subtitle += f" • Averaged across {iterations} iterations"
        
        # Load templates
        html_template = self._load_template('report.html')
        css_content = self.generate_css()
        chart_init_js = self._load_template('chart-init.js')
        table_sort_js = self._load_template('table-sort.js')
        
        # Build content sections
        content_sections = ""
        
        # Thread Configuration Analysis Charts (new section)
        content_sections += self._generate_thread_analysis_charts(results)
        
        # Performance Summary Table
        content_sections += self._generate_performance_summary(results)
        
        # Comparison Analysis
        content_sections += self._generate_comparison_analysis(results)
        
        # Detailed Latency Analysis
        content_sections += self._generate_latency_analysis(results)
        
        # Configuration Details
        content_sections += self._generate_configuration_section(results)
        
        # Generate chart JavaScript
        thread_data = self._extract_thread_data(results)
        chart_data_js = self._generate_chart_javascript(thread_data) if thread_data else ""
        
        # Assemble final HTML using template
        html_content = html_template.format(
            run_id=run_id,
            subtitle=subtitle,
            timestamp=timestamp,
            css_content=css_content,
            content_sections=content_sections,
            chart_init_js=chart_init_js,
            chart_data_js=chart_data_js,
            table_sort_js=table_sort_js
        )
        
        return html_content
    
    def _generate_thread_analysis_charts(self, results: Dict[str, Any]) -> str:
        """Generate interactive charts showing throughput and P50 latency by thread configuration"""
        # Extract thread configuration data
        thread_data = self._extract_thread_data(results)
        
        if not thread_data:
            return ""
        
        html = """
            <div class="section">
                <h2>📈 Thread Configuration Analysis</h2>
                <div class="chart-grid">
"""
        
        # Generate charts by mode, with separate charts for RAW and combined gRPC/Thrift
        protocols = set(item['protocol'] for item in thread_data)
        modes = set(item['mode'] for item in thread_data)
        
        chart_id = 0
        
        # RAW throughput chart (separate due to different scale/thread range)
        if any(item['protocol'] == 'raw' for item in thread_data):
            chart_id += 1
            html += f"""
                    <div class="chart-container">
                        <div class="chart-title">RAW Protocol - Throughput vs Thread Count</div>
                        <canvas id="throughput-chart-{chart_id}"></canvas>
                    </div>
"""
        
        # Combined gRPC+Thrift throughput chart
        if any(item['protocol'] in ['grpc', 'thrift'] for item in thread_data):
            chart_id += 1
            html += f"""
                    <div class="chart-container">
                        <div class="chart-title">gRPC vs Thrift - Throughput vs Thread Count</div>
                        <canvas id="throughput-chart-{chart_id}"></canvas>
                    </div>
"""
        
        # RAW P50 latency chart (separate due to different scale/thread range)
        if any(item['protocol'] == 'raw' for item in thread_data):
            chart_id += 1
            html += f"""
                    <div class="chart-container">
                        <div class="chart-title">RAW Protocol - P50 Latency vs Thread Count</div>
                        <canvas id="latency-chart-{chart_id}"></canvas>
                    </div>
"""
        
        # Combined gRPC+Thrift P50 latency chart
        if any(item['protocol'] in ['grpc', 'thrift'] for item in thread_data):
            chart_id += 1
            html += f"""
                    <div class="chart-container">
                        <div class="chart-title">gRPC vs Thrift - P50 Latency vs Thread Count</div>
                        <canvas id="latency-chart-{chart_id}"></canvas>
                    </div>
"""
        
        html += """
                </div>
            </div>
"""
        
        return html
    
    def _extract_thread_data(self, results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract thread configuration data from results"""
        thread_data = []
        
        for test_name, result in results.items():
            # Parse thread count from test name (e.g., "raw_read_100_8t" -> 8)
            thread_count = None
            if '_' in test_name:
                parts = test_name.split('_')
                for part in parts:
                    if part.endswith('t'):
                        try:
                            thread_count = int(part[:-1])  # Remove 't' suffix
                            break
                        except ValueError:
                            continue
            
            if thread_count is None:
                continue
            
            config = result['config']
            overall_stats = self._get_overall_stats(result)
            
            if not overall_stats:
                continue
                
            thread_data.append({
                'test_name': test_name,
                'protocol': config['protocol'],
                'mode': config['mode'],
                'threads': thread_count,
                'throughput': overall_stats['throughput'],
                'p50_latency': overall_stats['p50_latency'] / 1000000,  # Convert ns to ms
            })
        
        return sorted(thread_data, key=lambda x: (x['protocol'], x['mode'], x['threads']))
    
    def _generate_chart_javascript(self, thread_data: List[Dict[str, Any]]) -> str:
        """Generate JavaScript code for Chart.js charts"""
        if not thread_data:
            return ""
        
        js_code = ""
        chart_id = 0
        
        protocols = set(item['protocol'] for item in thread_data)
        modes = set(item['mode'] for item in thread_data)
        
        # Color scheme for different protocols
        protocol_colors = {
            'raw': 'rgba(46, 204, 113, 0.8)',
            'grpc': 'rgba(155, 89, 182, 0.8)', 
            'thrift': 'rgba(52, 152, 219, 0.8)'
        }
        
        # Enhanced colors for better differentiation
        protocol_mode_colors = {
            ('raw', 'read'): 'rgba(46, 204, 113, 0.9)',      # Green
            ('raw', 'write'): 'rgba(39, 174, 96, 0.9)',      # Darker Green
            ('raw', 'mixed'): 'rgba(22, 160, 133, 0.9)',     # Teal Green
            ('grpc', 'read'): 'rgba(155, 89, 182, 0.9)',     # Purple
            ('grpc', 'write'): 'rgba(142, 68, 173, 0.9)',    # Darker Purple
            ('grpc', 'mixed'): 'rgba(125, 60, 152, 0.9)',    # Deep Purple
            ('thrift', 'read'): 'rgba(52, 152, 219, 0.9)',   # Blue
            ('thrift', 'write'): 'rgba(41, 128, 185, 0.9)',  # Darker Blue
            ('thrift', 'mixed'): 'rgba(30, 100, 140, 0.9)',  # Deep Blue
        }
        
        # Line dash patterns for different modes
        dash_patterns = {
            'read': [],         # Solid line
            'write': [10, 5],   # Dashed line
            'mixed': [5, 5]     # Dotted line
        }
        
        # Generate RAW throughput chart
        raw_data = [item for item in thread_data if item['protocol'] == 'raw']
        if raw_data:
            chart_id += 1
            raw_datasets = []
            raw_threads = sorted(set(item['threads'] for item in raw_data))
            
            for mode in sorted(modes):
                mode_data = [item for item in raw_data if item['mode'] == mode]
                if not mode_data:
                    continue
                
                # Create data array aligned with raw_threads
                throughput_data = []
                for thread_count in raw_threads:
                    matching_point = next((item for item in mode_data if item['threads'] == thread_count), None)
                    throughput_data.append(matching_point['throughput'] if matching_point else None)
                
                color = protocol_mode_colors.get(('raw', mode), 'rgba(128, 128, 128, 0.8)')
                dash_pattern = dash_patterns.get(mode, [])
                
                raw_datasets.append(f"""{{
                    label: '{mode.upper()}',
                    data: {json.dumps(throughput_data)},
                    borderColor: '{color}',
                    backgroundColor: '{color.replace("0.9", "0.2")}',
                    borderWidth: 3,
                    pointRadius: 5,
                    pointHoverRadius: 7,
                    tension: 0.3,
                    fill: false,
                    borderDash: {json.dumps(dash_pattern)},
                    spanGaps: true
                }}""")
            
            js_code += f"""
            // RAW throughput chart
            const throughputCtx{chart_id} = document.getElementById('throughput-chart-{chart_id}').getContext('2d');
            new Chart(throughputCtx{chart_id}, {{
                type: 'line',
                data: {{
                    labels: {json.dumps(raw_threads)},
                    datasets: [
                        {','.join(raw_datasets)}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: {{
                        intersect: false,
                        mode: 'index'
                    }},
                    plugins: {{
                        legend: {{
                            display: true,
                            position: 'top',
                            labels: {{
                                usePointStyle: true,
                                padding: 20,
                                font: {{
                                    size: 12
                                }}
                            }}
                        }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    if (context.parsed.y === null) return null;
                                    return 'RAW ' + context.dataset.label + ': ' + context.parsed.y.toLocaleString() + ' ops/s';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            display: true,
                            title: {{
                                display: true,
                                text: 'Thread Count',
                                font: {{
                                    size: 14,
                                    weight: 'bold'
                                }}
                            }},
                            grid: {{
                                display: true,
                                color: 'rgba(200, 200, 200, 0.3)'
                            }}
                        }},
                        y: {{
                            display: true,
                            title: {{
                                display: true,
                                text: 'Throughput (ops/s)',
                                font: {{
                                    size: 14,
                                    weight: 'bold'
                                }}
                            }},
                            grid: {{
                                display: true,
                                color: 'rgba(200, 200, 200, 0.3)'
                            }},
                            ticks: {{
                                callback: function(value) {{
                                    return value.toLocaleString();
                                }}
                            }}
                        }}
                    }}
                }}
            }});
"""
        
        # Generate combined gRPC+Thrift throughput chart
        network_data = [item for item in thread_data if item['protocol'] in ['grpc', 'thrift']]
        if network_data:
            chart_id += 1
            network_datasets = []
            network_threads = sorted(set(item['threads'] for item in network_data))
            
            for protocol in ['grpc', 'thrift']:
                for mode in sorted(modes):
                    mode_data = [item for item in network_data if item['protocol'] == protocol and item['mode'] == mode]
                    if not mode_data:
                        continue
                    
                    # Create data array aligned with network_threads
                    throughput_data = []
                    for thread_count in network_threads:
                        matching_point = next((item for item in mode_data if item['threads'] == thread_count), None)
                        throughput_data.append(matching_point['throughput'] if matching_point else None)
                    
                    color = protocol_mode_colors.get((protocol, mode), 'rgba(128, 128, 128, 0.8)')
                    dash_pattern = dash_patterns.get(mode, [])
                    
                    network_datasets.append(f"""{{
                        label: '{protocol.upper()} - {mode.upper()}',
                        data: {json.dumps(throughput_data)},
                        borderColor: '{color}',
                        backgroundColor: '{color.replace("0.9", "0.2")}',
                        borderWidth: 3,
                        pointRadius: 5,
                        pointHoverRadius: 7,
                        tension: 0.3,
                        fill: false,
                        borderDash: {json.dumps(dash_pattern)},
                        spanGaps: true
                    }}""")
            
            js_code += f"""
            // Combined gRPC+Thrift throughput chart
            const throughputCtx{chart_id} = document.getElementById('throughput-chart-{chart_id}').getContext('2d');
            new Chart(throughputCtx{chart_id}, {{
                type: 'line',
                data: {{
                    labels: {json.dumps(network_threads)},
                    datasets: [
                        {','.join(network_datasets)}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: {{
                        intersect: false,
                        mode: 'index'
                    }},
                    plugins: {{
                        legend: {{
                            display: true,
                            position: 'top',
                            labels: {{
                                usePointStyle: true,
                                padding: 20,
                                font: {{
                                    size: 12
                                }}
                            }}
                        }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    if (context.parsed.y === null) return null;
                                    return context.dataset.label + ': ' + context.parsed.y.toLocaleString() + ' ops/s';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            display: true,
                            title: {{
                                display: true,
                                text: 'Thread Count',
                                font: {{
                                    size: 14,
                                    weight: 'bold'
                                }}
                            }},
                            grid: {{
                                display: true,
                                color: 'rgba(200, 200, 200, 0.3)'
                            }}
                        }},
                        y: {{
                            display: true,
                            title: {{
                                display: true,
                                text: 'Throughput (ops/s)',
                                font: {{
                                    size: 14,
                                    weight: 'bold'
                                }}
                            }},
                            grid: {{
                                display: true,
                                color: 'rgba(200, 200, 200, 0.3)'
                            }},
                            ticks: {{
                                callback: function(value) {{
                                    return value.toLocaleString();
                                }}
                            }}
                        }}
                    }}
                }}
            }});
"""
        
        # Generate RAW P50 latency chart
        raw_data = [item for item in thread_data if item['protocol'] == 'raw']
        if raw_data:
            chart_id += 1
            raw_latency_datasets = []
            raw_threads = sorted(set(item['threads'] for item in raw_data))
            
            for mode in sorted(modes):
                mode_data = [item for item in raw_data if item['mode'] == mode]
                if not mode_data:
                    continue
                
                # Create data array aligned with raw_threads
                latency_data = []
                for thread_count in raw_threads:
                    matching_point = next((item for item in mode_data if item['threads'] == thread_count), None)
                    latency_data.append(matching_point['p50_latency'] if matching_point else None)
                
                color = protocol_mode_colors.get(('raw', mode), 'rgba(128, 128, 128, 0.8)')
                dash_pattern = dash_patterns.get(mode, [])
                
                raw_latency_datasets.append(f"""{{
                    label: '{mode.upper()}',
                    data: {json.dumps(latency_data)},
                    borderColor: '{color}',
                    backgroundColor: '{color.replace("0.9", "0.2")}',
                    borderWidth: 3,
                    pointRadius: 5,
                    pointHoverRadius: 7,
                    tension: 0.3,
                    fill: false,
                    borderDash: {json.dumps(dash_pattern)},
                    spanGaps: true
                }}""")
            
            js_code += f"""
            // RAW P50 latency chart
            const latencyCtx{chart_id} = document.getElementById('latency-chart-{chart_id}').getContext('2d');
            new Chart(latencyCtx{chart_id}, {{
                type: 'line',
                data: {{
                    labels: {json.dumps(raw_threads)},
                    datasets: [
                        {','.join(raw_latency_datasets)}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: {{
                        intersect: false,
                        mode: 'index'
                    }},
                    plugins: {{
                        legend: {{
                            display: true,
                            position: 'top',
                            labels: {{
                                usePointStyle: true,
                                padding: 20,
                                font: {{
                                    size: 12
                                }}
                            }}
                        }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    if (context.parsed.y === null) return null;
                                    return 'RAW ' + context.dataset.label + ': ' + context.parsed.y.toFixed(2) + ' ms';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            display: true,
                            title: {{
                                display: true,
                                text: 'Thread Count',
                                font: {{
                                    size: 14,
                                    weight: 'bold'
                                }}
                            }},
                            grid: {{
                                display: true,
                                color: 'rgba(200, 200, 200, 0.3)'
                            }}
                        }},
                        y: {{
                            display: true,
                            title: {{
                                display: true,
                                text: 'P50 Latency (ms)',
                                font: {{
                                    size: 14,
                                    weight: 'bold'
                                }}
                            }},
                            grid: {{
                                display: true,
                                color: 'rgba(200, 200, 200, 0.3)'
                            }},
                            ticks: {{
                                callback: function(value) {{
                                    return value.toFixed(2);
                                }}
                            }}
                        }}
                    }}
                }}
            }});
"""
        
        # Generate combined gRPC+Thrift P50 latency chart
        network_data = [item for item in thread_data if item['protocol'] in ['grpc', 'thrift']]
        if network_data:
            chart_id += 1
            network_latency_datasets = []
            network_threads = sorted(set(item['threads'] for item in network_data))
            
            for protocol in ['grpc', 'thrift']:
                for mode in sorted(modes):
                    mode_data = [item for item in network_data if item['protocol'] == protocol and item['mode'] == mode]
                    if not mode_data:
                        continue
                    
                    # Create data array aligned with network_threads
                    latency_data = []
                    for thread_count in network_threads:
                        matching_point = next((item for item in mode_data if item['threads'] == thread_count), None)
                        latency_data.append(matching_point['p50_latency'] if matching_point else None)
                    
                    color = protocol_mode_colors.get((protocol, mode), 'rgba(128, 128, 128, 0.8)')
                    dash_pattern = dash_patterns.get(mode, [])
                    
                    network_latency_datasets.append(f"""{{
                        label: '{protocol.upper()} - {mode.upper()}',
                        data: {json.dumps(latency_data)},
                        borderColor: '{color}',
                        backgroundColor: '{color.replace("0.9", "0.2")}',
                        borderWidth: 3,
                        pointRadius: 5,
                        pointHoverRadius: 7,
                        tension: 0.3,
                        fill: false,
                        borderDash: {json.dumps(dash_pattern)},
                        spanGaps: true
                    }}""")
            
            js_code += f"""
            // Combined gRPC+Thrift P50 latency chart
            const latencyCtx{chart_id} = document.getElementById('latency-chart-{chart_id}').getContext('2d');
            new Chart(latencyCtx{chart_id}, {{
                type: 'line',
                data: {{
                    labels: {json.dumps(network_threads)},
                    datasets: [
                        {','.join(network_latency_datasets)}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: {{
                        intersect: false,
                        mode: 'index'
                    }},
                    plugins: {{
                        legend: {{
                            display: true,
                            position: 'top',
                            labels: {{
                                usePointStyle: true,
                                padding: 20,
                                font: {{
                                    size: 12
                                }}
                            }}
                        }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    if (context.parsed.y === null) return null;
                                    return context.dataset.label + ': ' + context.parsed.y.toFixed(2) + ' ms';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            display: true,
                            title: {{
                                display: true,
                                text: 'Thread Count',
                                font: {{
                                    size: 14,
                                    weight: 'bold'
                                }}
                            }},
                            grid: {{
                                display: true,
                                color: 'rgba(200, 200, 200, 0.3)'
                            }}
                        }},
                        y: {{
                            display: true,
                            title: {{
                                display: true,
                                text: 'P50 Latency (ms)',
                                font: {{
                                    size: 14,
                                    weight: 'bold'
                                }}
                            }},
                            grid: {{
                                display: true,
                                color: 'rgba(200, 200, 200, 0.3)'
                            }},
                            ticks: {{
                                callback: function(value) {{
                                    return value.toFixed(2);
                                }}
                            }}
                        }}
                    }}
                }}
            }});
"""
        
        return js_code
    
    def _generate_performance_summary(self, results: Dict[str, Any]) -> str:
        """Generate performance summary table"""
        html = """
            <div class="section">
                <h2>📊 Performance Summary</h2>
                <table id="performance-summary-table">
                    <thead>
                        <tr>
                            <th class="sortable" data-sort="string">Test Case</th>
                            <th class="sortable" data-sort="string">Protocol</th>
                            <th class="sortable" data-sort="string">Mode</th>
                            <th class="sortable" data-sort="number">Total Throughput</th>
                            <th class="sortable" data-sort="number">Read Throughput</th>
                            <th class="sortable" data-sort="number">Write Throughput</th>
                            <th class="sortable" data-sort="duration">Read Avg Latency</th>
                            <th class="sortable" data-sort="duration">Write Avg Latency</th>
                            <th class="sortable" data-sort="duration">Overall Avg Latency</th>
                            <th class="sortable" data-sort="number">Success Rate</th>
                            <th class="sortable" data-sort="number">Total Requests</th>
                        </tr>
                    </thead>
                    <tbody>
"""
        
        # Sort results by throughput (descending)
        sorted_results = []
        for test_name, result in results.items():
            overall_stats = self._get_overall_stats(result)
            if overall_stats:
                sorted_results.append((test_name, result, overall_stats))
        
        sorted_results.sort(key=lambda x: x[2]['throughput'], reverse=True)
        
        for test_name, result, overall_stats in sorted_results:
            config = result['config']
            protocol = config['protocol']
            mode = config['mode']
            
            # Get individual read/write throughput and latency
            read_stats = self._get_operation_stats(result, 'READ')
            write_stats = self._get_operation_stats(result, 'WRITE')
            
            read_throughput = read_stats['throughput'] if read_stats else 0.0
            write_throughput = write_stats['throughput'] if write_stats else 0.0
            
            throughput = overall_stats['throughput']
            overall_avg_latency = self.format_duration(overall_stats['avg_latency'])
            success_rate = overall_stats['success']/overall_stats['count']*100
            
            protocol_class = f"protocol-{protocol.lower()}"
            throughput_class = self.get_throughput_class(throughput)
            
            # Format individual throughput and latency values
            read_throughput_str = f"{self.format_number(read_throughput)} ops/s" if read_throughput > 0 else "N/A"
            write_throughput_str = f"{self.format_number(write_throughput)} ops/s" if write_throughput > 0 else "N/A"
            read_avg_latency_str = self.format_duration(read_stats['avg_latency']) if read_stats else "N/A"
            write_avg_latency_str = self.format_duration(write_stats['avg_latency']) if write_stats else "N/A"
            
            html += f"""
                        <tr>
                            <td><strong>{test_name}</strong></td>
                            <td><span class="{protocol_class}">{protocol.upper()}</span></td>
                            <td>{mode.upper()}</td>
                            <td class="{throughput_class}">{self.format_number(throughput)} ops/s</td>
                            <td>{read_throughput_str}</td>
                            <td>{write_throughput_str}</td>
                            <td>{read_avg_latency_str}</td>
                            <td>{write_avg_latency_str}</td>
                            <td>{overall_avg_latency}</td>
                            <td>{success_rate:.1f}%</td>
                            <td>{self.format_number(overall_stats['count'])}</td>
                        </tr>
"""
        
        html += """
                    </tbody>
                </table>
            </div>
"""
        return html
    
    def _generate_comparison_analysis(self, results: Dict[str, Any]) -> str:
        """Generate protocol and workload comparison"""
        protocol_stats = {}
        mode_stats = {}
        
        # Collect statistics
        for test_name, result in results.items():
            config = result['config']
            protocol = config['protocol']
            mode = config['mode']
            
            overall_stats = self._get_overall_stats(result)
            if not overall_stats:
                continue
            
            if protocol not in protocol_stats:
                protocol_stats[protocol] = []
            protocol_stats[protocol].append(overall_stats)
            
            if mode not in mode_stats:
                mode_stats[mode] = []
            mode_stats[mode].append(overall_stats)
        
        html = """
            <div class="section">
                <h2>📈 Comparison Analysis</h2>
                
                <h3>Protocol Comparison</h3>
                <div class="stats-grid">
"""
        
        # Protocol comparison cards
        for protocol, stats_list in protocol_stats.items():
            if stats_list:
                avg_throughput = sum(s['throughput'] for s in stats_list) / len(stats_list)
                avg_latency = sum(s['avg_latency'] for s in stats_list) / len(stats_list)
                total_ops = sum(s['count'] for s in stats_list)
                
                html += f"""
                    <div class="stat-card">
                        <h4>{protocol.upper()} Protocol</h4>
                        <div class="stat-value">{self.format_number(avg_throughput)}</div>
                        <div class="stat-description">ops/s avg • {self.format_duration(int(avg_latency))} avg latency</div>
                        <div class="stat-description">{len(stats_list)} tests • {self.format_number(total_ops)} total ops</div>
                    </div>
"""
        
        html += """
                </div>
                
                <h3>Workload Comparison</h3>
                <div class="stats-grid">
"""
        
        # Workload comparison cards
        for mode, stats_list in mode_stats.items():
            if stats_list:
                avg_throughput = sum(s['throughput'] for s in stats_list) / len(stats_list)
                avg_latency = sum(s['avg_latency'] for s in stats_list) / len(stats_list)
                total_ops = sum(s['count'] for s in stats_list)
                
                # Calculate average read/write throughput for mixed workloads
                mode_description = f"ops/s avg • {self.format_duration(int(avg_latency))} avg latency"
                if mode.lower() == 'mixed':
                    # Get average read/write throughput for mixed workloads
                    avg_read_tput = 0.0
                    avg_write_tput = 0.0
                    mixed_tests = 0
                    
                    for test_name, result in results.items():
                        if result['config']['mode'].lower() == 'mixed':
                            read_tput = self._get_operation_throughput(result, 'READ')
                            write_tput = self._get_operation_throughput(result, 'WRITE')
                            if read_tput > 0 or write_tput > 0:
                                avg_read_tput += read_tput
                                avg_write_tput += write_tput
                                mixed_tests += 1
                    
                    if mixed_tests > 0:
                        avg_read_tput /= mixed_tests
                        avg_write_tput /= mixed_tests
                        mode_description = f"Read: {self.format_number(avg_read_tput)} ops/s • Write: {self.format_number(avg_write_tput)} ops/s"
                
                html += f"""
                    <div class="stat-card">
                        <h4>{mode.upper()} Workload</h4>
                        <div class="stat-value">{self.format_number(avg_throughput)}</div>
                        <div class="stat-description">{mode_description}</div>
                        <div class="stat-description">{len(stats_list)} tests • {self.format_number(total_ops)} total ops</div>
                    </div>
"""
        
        html += """
                </div>
            </div>
"""
        return html
    
    def _generate_latency_analysis(self, results: Dict[str, Any]) -> str:
        """Generate detailed latency analysis table"""
        html = """
            <div class="section">
                <h2>⚡ Detailed Latency Analysis</h2>
                
                <h3>Read Operation Latencies</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Test Case</th>
                            <th>Min</th>
                            <th>P50</th>
                            <th>P90</th>
                            <th>P95</th>
                            <th>P99</th>
                            <th>Max</th>
                        </tr>
                    </thead>
                    <tbody>
"""
        
        # Read latencies table
        for test_name, result in results.items():
            read_stats = self._get_operation_stats(result, 'READ')
            if not read_stats:
                continue
            
            html += f"""
                        <tr>
                            <td><strong>{test_name}</strong></td>
                            <td>{self.format_duration(read_stats['min_latency'])}</td>
                            <td>{self.format_duration(read_stats['p50_latency'])}</td>
                            <td>{self.format_duration(read_stats['p90_latency'])}</td>
                            <td>{self.format_duration(read_stats['p95_latency'])}</td>
                            <td>{self.format_duration(read_stats['p99_latency'])}</td>
                            <td>{self.format_duration(read_stats['max_latency'])}</td>
                        </tr>
"""
        
        html += """
                    </tbody>
                </table>
                
                <h3>Write Operation Latencies</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Test Case</th>
                            <th>Min</th>
                            <th>P50</th>
                            <th>P90</th>
                            <th>P95</th>
                            <th>P99</th>
                            <th>Max</th>
                        </tr>
                    </thead>
                    <tbody>
"""
        
        # Write latencies table
        for test_name, result in results.items():
            write_stats = self._get_operation_stats(result, 'WRITE')
            if not write_stats:
                continue
            
            html += f"""
                        <tr>
                            <td><strong>{test_name}</strong></td>
                            <td>{self.format_duration(write_stats['min_latency'])}</td>
                            <td>{self.format_duration(write_stats['p50_latency'])}</td>
                            <td>{self.format_duration(write_stats['p90_latency'])}</td>
                            <td>{self.format_duration(write_stats['p95_latency'])}</td>
                            <td>{self.format_duration(write_stats['p99_latency'])}</td>
                            <td>{self.format_duration(write_stats['max_latency'])}</td>
                        </tr>
"""
        
        html += """
                    </tbody>
                </table>
                
                <h3>Overall Latencies</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Test Case</th>
                            <th>Min</th>
                            <th>P50</th>
                            <th>P90</th>
                            <th>P95</th>
                            <th>P99</th>
                            <th>Max</th>
                        </tr>
                    </thead>
                    <tbody>
"""
        
        # Overall latencies table
        for test_name, result in results.items():
            overall_stats = self._get_overall_stats(result)
            if not overall_stats:
                continue
            
            html += f"""
                        <tr>
                            <td><strong>{test_name}</strong></td>
                            <td>{self.format_duration(overall_stats['min_latency'])}</td>
                            <td>{self.format_duration(overall_stats['p50_latency'])}</td>
                            <td>{self.format_duration(overall_stats['p90_latency'])}</td>
                            <td>{self.format_duration(overall_stats['p95_latency'])}</td>
                            <td>{self.format_duration(overall_stats['p99_latency'])}</td>
                            <td>{self.format_duration(overall_stats['max_latency'])}</td>
                        </tr>
"""
        
        html += """
                    </tbody>
                </table>
            </div>
"""
        return html
    
    def _generate_configuration_section(self, results: Dict[str, Any]) -> str:
        """Generate test configuration section"""
        if not results:
            return ""
        
        # Get configuration from first result
        sample_result = list(results.values())[0]
        config = sample_result['config']
        start_time = sample_result['start_time']
        total_duration = sample_result['total_duration']
        
        # Check if results are averaged
        is_averaged = config.get('averaged', False)
        iterations = config.get('iterations', 1)
        
        html = f"""
            <div class="section">
                <h2>⚙️ Test Configuration</h2>"""
        
        # Add averaging info if applicable
        if is_averaged:
            html += f"""
                <div class="alert alert-info">
                    <strong>📊 Averaged Results:</strong> These results are averaged across {iterations} iterations for improved reliability.
                </div>"""
        
        html += f"""
                <div class="config-section">
                    <div class="stats-grid">
                        <div class="stat-card">
                            <h4>Total Requests</h4>
                            <div class="stat-value">{self.format_number(config['total_requests'])}</div>
                            <div class="stat-description">per test case</div>
                        </div>"""
        
        # Add iterations card if averaged
        if is_averaged:
            html += f"""
                        <div class="stat-card">
                            <h4>Iterations</h4>
                            <div class="stat-value">{iterations}</div>
                            <div class="stat-description">averaged runs</div>
                        </div>"""
        
        html += f"""
                        <div class="stat-card">
                            <h4>Concurrent Threads</h4>
                            <div class="stat-value">{config['num_threads']}</div>
                            <div class="stat-description">parallel workers</div>
                        </div>
                        <div class="stat-card">
                            <h4>Key Size</h4>
                            <div class="stat-value">{config['key_size']}</div>
                            <div class="stat-description">bytes</div>
                        </div>
                        <div class="stat-card">
                            <h4>Value Size</h4>
                            <div class="stat-value">{config['value_size']}</div>
                            <div class="stat-description">bytes</div>
                        </div>
                        <div class="stat-card">
                            <h4>Test Duration</h4>
                            <div class="stat-value">{self.format_duration(total_duration)}</div>
                            <div class="stat-description">{'avg per iteration' if is_averaged else 'total runtime'}</div>
                        </div>
                        <div class="stat-card">
                            <h4>Started At</h4>
                            <div class="stat-value" style="font-size: 1.4em;">{start_time.replace('T', ' ').replace('Z', '').split('.')[0]}</div>
                            <div class="stat-description">UTC time</div>
                        </div>
                    </div>
                </div>
            </div>
"""
        return html
    
    def _get_overall_stats(self, result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract overall statistics from a result"""
        for stat in result.get('statistics', []):
            if stat['operation'] == 'OVERALL':
                return stat
        return None
    
    def _get_operation_throughput(self, result: Dict[str, Any], operation: str) -> float:
        """Extract throughput for a specific operation (READ or WRITE)"""
        for stat in result.get('statistics', []):
            if stat['operation'] == operation:
                return stat.get('throughput', 0.0)
        return 0.0
    
    def _get_operation_stats(self, result: Dict[str, Any], operation: str) -> Optional[Dict[str, Any]]:
        """Extract full statistics for a specific operation (READ, WRITE, or OVERALL)"""
        for stat in result.get('statistics', []):
            if stat['operation'] == operation:
                return stat
        return None
    
    def print_console_analysis(self, results: Dict[str, Any]):
        """Print analysis to console (similar to bash script output)"""
        print("=" * 80)
        print("COMPREHENSIVE BENCHMARK ANALYSIS")
        print("=" * 80)
        
        # Check if results are averaged
        sample_result = list(results.values())[0] if results else None
        if sample_result:
            config = sample_result['config']
            is_averaged = config.get('averaged', False)
            iterations = config.get('iterations', 1)
            
            if is_averaged:
                print(f"📊 Results are averaged across {iterations} iterations for improved reliability")
                print()
        
        print()
        
        # Performance Summary
        print("PERFORMANCE SUMMARY")
        print("-" * 150)
        print(f"{'Test Case':<25} {'Protocol':<10} {'Mode':<10} {'Total Tput':<15} {'Read Tput':<15} {'Write Tput':<15} {'Read Avg Lat':<15} {'Write Avg Lat':<15} {'Overall Avg Lat':<15} {'Success Rate':<12}")
        print("-" * 150)
        
        protocol_stats = {}
        mode_stats = {}
        
        for test_name, result in results.items():
            config = result['config']
            protocol = config['protocol']
            mode = config['mode']
            
            overall_stats = self._get_overall_stats(result)
            if not overall_stats:
                continue
            
            # Get individual read/write statistics
            read_stats = self._get_operation_stats(result, 'READ')
            write_stats = self._get_operation_stats(result, 'WRITE')
            
            read_throughput = read_stats['throughput'] if read_stats else 0.0
            write_throughput = write_stats['throughput'] if write_stats else 0.0
                
            throughput = f"{overall_stats['throughput']:.0f} ops/s"
            read_tput_str = f"{read_throughput:.0f} ops/s" if read_throughput > 0 else "N/A"
            write_tput_str = f"{write_throughput:.0f} ops/s" if write_throughput > 0 else "N/A"
            
            # Format latencies
            read_avg_latency = self.format_duration(read_stats['avg_latency']) if read_stats else "N/A"
            write_avg_latency = self.format_duration(write_stats['avg_latency']) if write_stats else "N/A"
            overall_avg_latency = self.format_duration(overall_stats['avg_latency'])
            success_rate = f"{(overall_stats['success']/overall_stats['count']*100):.1f}%"
            
            print(f"{test_name:<25} {protocol:<10} {mode:<10} {throughput:<15} {read_tput_str:<15} {write_tput_str:<15} {read_avg_latency:<15} {write_avg_latency:<15} {overall_avg_latency:<15} {success_rate:<12}")
            
            # Collect stats for grouping
            if protocol not in protocol_stats:
                protocol_stats[protocol] = []
            protocol_stats[protocol].append(overall_stats)
            
            if mode not in mode_stats:
                mode_stats[mode] = []
            mode_stats[mode].append(overall_stats)
        
        print()
        
        # Protocol comparison
        print("PROTOCOL COMPARISON")
        print("-" * 50)
        for protocol, stats_list in protocol_stats.items():
            if stats_list:
                avg_throughput = sum(s['throughput'] for s in stats_list) / len(stats_list)
                avg_latency = sum(s['avg_latency'] for s in stats_list) / len(stats_list)
                print(f"{protocol.upper():<10} Avg Throughput: {avg_throughput:.0f} ops/s, Avg Latency: {self.format_duration(int(avg_latency))}")
        
        print()
        
        # Mode comparison  
        print("WORKLOAD COMPARISON")
        print("-" * 50)
        for mode, stats_list in mode_stats.items():
            if stats_list:
                avg_throughput = sum(s['throughput'] for s in stats_list) / len(stats_list)
                avg_latency = sum(s['avg_latency'] for s in stats_list) / len(stats_list)
                print(f"{mode.upper():<10} Avg Throughput: {avg_throughput:.0f} ops/s, Avg Latency: {self.format_duration(int(avg_latency))}")
        
        print()
        
        # Detailed latency analysis
        print("DETAILED LATENCY ANALYSIS")
        print("-" * 100)
        
        # Read latencies
        print("READ OPERATION LATENCIES")
        print("-" * 80)
        print(f"{'Test Case':<25} {'Min':<10} {'P50':<10} {'P90':<10} {'P95':<10} {'P99':<10} {'Max':<10}")
        print("-" * 80)
        
        for test_name, result in results.items():
            read_stats = self._get_operation_stats(result, 'READ')
            if not read_stats:
                continue
                
            min_lat = self.format_duration(read_stats['min_latency'])
            p50_lat = self.format_duration(read_stats['p50_latency'])
            p90_lat = self.format_duration(read_stats['p90_latency'])
            p95_lat = self.format_duration(read_stats['p95_latency'])
            p99_lat = self.format_duration(read_stats['p99_latency'])
            max_lat = self.format_duration(read_stats['max_latency'])
            
            print(f"{test_name:<25} {min_lat:<10} {p50_lat:<10} {p90_lat:<10} {p95_lat:<10} {p99_lat:<10} {max_lat:<10}")
        
        print()
        
        # Write latencies
        print("WRITE OPERATION LATENCIES")
        print("-" * 80)
        print(f"{'Test Case':<25} {'Min':<10} {'P50':<10} {'P90':<10} {'P95':<10} {'P99':<10} {'Max':<10}")
        print("-" * 80)
        
        for test_name, result in results.items():
            write_stats = self._get_operation_stats(result, 'WRITE')
            if not write_stats:
                continue
                
            min_lat = self.format_duration(write_stats['min_latency'])
            p50_lat = self.format_duration(write_stats['p50_latency'])
            p90_lat = self.format_duration(write_stats['p90_latency'])
            p95_lat = self.format_duration(write_stats['p95_latency'])
            p99_lat = self.format_duration(write_stats['p99_latency'])
            max_lat = self.format_duration(write_stats['max_latency'])
            
            print(f"{test_name:<25} {min_lat:<10} {p50_lat:<10} {p90_lat:<10} {p95_lat:<10} {p99_lat:<10} {max_lat:<10}")
        
        print()
        
        # Overall latencies
        print("OVERALL LATENCIES")
        print("-" * 80)
        print(f"{'Test Case':<25} {'Min':<10} {'P50':<10} {'P90':<10} {'P95':<10} {'P99':<10} {'Max':<10}")
        print("-" * 80)
        
        for test_name, result in results.items():
            overall_stats = self._get_overall_stats(result)
            if not overall_stats:
                continue
                
            min_lat = self.format_duration(overall_stats['min_latency'])
            p50_lat = self.format_duration(overall_stats['p50_latency'])
            p90_lat = self.format_duration(overall_stats['p90_latency'])
            p95_lat = self.format_duration(overall_stats['p95_latency'])
            p99_lat = self.format_duration(overall_stats['p99_latency'])
            max_lat = self.format_duration(overall_stats['max_latency'])
            
            print(f"{test_name:<25} {min_lat:<10} {p50_lat:<10} {p90_lat:<10} {p95_lat:<10} {p99_lat:<10} {max_lat:<10}")
        
        print()
        
        # Configuration summary
        print("TEST CONFIGURATION")
        print("-" * 50)
        if results:
            sample_config = list(results.values())[0]['config']
            print(f"Total Requests: {sample_config['total_requests']:,}")
            print(f"Concurrent Threads: {sample_config['num_threads']}")
            print(f"Key Size: {sample_config['key_size']} bytes")
            print(f"Value Size: {sample_config['value_size']} bytes")
            print(f"Timeout: {sample_config['timeout']/1000000000:.0f}s")

    def generate_report(self, input_path: str, output_file: Optional[str] = None, console_output: bool = False) -> str:
        """Generate HTML report from benchmark results"""
        results = self.load_single_run(input_path)
        
        # Print console analysis if requested
        if console_output:
            self.print_console_analysis(results)
        
        if not output_file:
            output_file = os.path.join(input_path, 'benchmark_report.html')
        
        html_content = self.generate_single_run_html(results, input_path)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return output_file
    
    def list_available_runs(self, base_dir: str = "../benchmark_results") -> List[str]:
        """List all available benchmark runs"""
        runs = []
        base_path = Path(base_dir)
        
        if not base_path.exists():
            return runs
        
        for item in base_path.iterdir():
            if item.is_dir() and item.name.startswith('run_'):
                # Check if directory contains JSON files
                json_files = list(item.glob('*.json'))
                if json_files:
                    runs.append(str(item))
        
        return sorted(runs, reverse=True)  # Most recent first


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Generate HTML reports from KV Store benchmark results',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate report for latest run
  python3 generate_benchmark_report.py ../benchmark_results/run_20250807_143052
  
  # Generate report with custom output file
  python3 generate_benchmark_report.py ../benchmark_results/run_20250807_143052 -o my_report.html
  
  # List all available runs
  python3 generate_benchmark_report.py --list
        """
    )
    
    parser.add_argument(
        'input_path',
        nargs='?',
        help='Path to benchmark results directory'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='Output HTML file path (default: benchmark_report.html in input directory)'
    )
    
    parser.add_argument(
        '--list',
        action='store_true',
        help='List all available benchmark runs'
    )
    
    parser.add_argument(
        '--base-dir',
        default='../benchmark_results',
        help='Base directory for benchmark results (default: ../benchmark_results)'
    )
    
    parser.add_argument(
        '--console',
        action='store_true',
        help='Also print analysis to console'
    )
    
    args = parser.parse_args()
    
    generator = BenchmarkReportGenerator()
    
    try:
        if args.list:
            # List available runs
            runs = generator.list_available_runs(args.base_dir)
            if runs:
                print("Available benchmark runs:")
                print("=" * 40)
                for run in runs:
                    run_name = os.path.basename(run)
                    # Parse timestamp from run name
                    if run_name.startswith('run_'):
                        timestamp_str = run_name[4:]  # Remove 'run_' prefix
                        try:
                            # Convert YYYYMMDD_HHMMSS to readable format
                            dt = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                            formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                            print(f"  {run_name} ({formatted_time})")
                        except ValueError:
                            print(f"  {run_name}")
                    else:
                        print(f"  {run_name}")
                print(f"\nTotal: {len(runs)} runs found")
                
                # Show example command for latest run
                if runs:
                    latest_run = runs[0]
                    print(f"\nTo generate report for latest run:")
                    print(f"  python3 {sys.argv[0]} {latest_run}")
            else:
                print(f"No benchmark runs found in {args.base_dir}")
            return
        
        if not args.input_path:
            # Try to use the latest run
            runs = generator.list_available_runs(args.base_dir)
            if runs:
                args.input_path = runs[0]
                print(f"Using latest run: {os.path.basename(args.input_path)}")
            else:
                print("Error: No input path specified and no benchmark runs found")
                print(f"Run with --list to see available runs or specify a path")
                sys.exit(1)
        
        # Generate the report
        output_file = generator.generate_report(args.input_path, args.output, console_output=args.console)
        
        print("✅ HTML report generated successfully!")
        print(f"📊 Report saved to: {output_file}")
        print(f"🌐 Open in browser: file://{os.path.abspath(output_file)}")
        
        # Show file size
        file_size = os.path.getsize(output_file)
        if file_size > 1024 * 1024:
            size_str = f"{file_size / (1024 * 1024):.1f} MB"
        elif file_size > 1024:
            size_str = f"{file_size / 1024:.1f} KB"
        else:
            size_str = f"{file_size} bytes"
        print(f"📁 File size: {size_str}")
        
    except (FileNotFoundError, ValueError) as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n❌ Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()