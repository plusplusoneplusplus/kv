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


class BenchmarkReportGenerator:
    """Generate HTML reports from benchmark JSON results"""
    
    def __init__(self):
        self.results: Dict[str, Any] = {}
        self.comparison_mode = False
        self.run_dirs: List[str] = []
    
    def load_single_run(self, results_dir: str) -> Dict[str, Any]:
        """Load all JSON result files from a single run directory"""
        results = {}
        results_path = Path(results_dir)
        
        if not results_path.exists():
            raise FileNotFoundError(f"Results directory '{results_dir}' not found")
        
        json_files = list(results_path.glob('*.json'))
        if not json_files:
            raise ValueError(f"No JSON files found in '{results_dir}'")
        
        for json_file in json_files:
            test_name = json_file.stem
            try:
                with open(json_file, 'r') as f:
                    results[test_name] = json.load(f)
            except json.JSONDecodeError as e:
                print(f"Warning: Failed to parse {json_file}: {e}")
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
    
    def generate_css(self) -> str:
        """Generate CSS styles for the HTML report"""
        return """
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        
        .header {
            background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }
        
        .header h1 {
            font-size: 2.8em;
            margin-bottom: 15px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        
        .header .subtitle {
            font-size: 1.3em;
            opacity: 0.9;
            margin-bottom: 10px;
        }
        
        .header .timestamp {
            font-size: 1em;
            opacity: 0.7;
        }
        
        .content {
            padding: 40px;
        }
        
        .section {
            margin-bottom: 50px;
        }
        
        .section h2 {
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 15px;
            margin-bottom: 25px;
            font-size: 2em;
        }
        
        .section h3 {
            color: #34495e;
            margin: 30px 0 20px 0;
            font-size: 1.5em;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 25px 0;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            border-radius: 10px;
            overflow: hidden;
        }
        
        th {
            background: linear-gradient(135deg, #3498db 0%, #2980b9 100%);
            color: white;
            font-weight: 600;
            padding: 18px 15px;
            text-align: left;
            font-size: 0.95em;
            text-transform: uppercase;
            letter-spacing: 0.8px;
        }
        
        th.sortable {
            cursor: pointer;
            user-select: none;
            position: relative;
            transition: background-color 0.3s ease;
        }
        
        th.sortable:hover {
            background: linear-gradient(135deg, #2980b9 0%, #1f639a 100%);
        }
        
        th.sortable:after {
            content: ' ↕️';
            font-size: 0.8em;
            opacity: 0.6;
        }
        
        th.sortable.sort-asc:after {
            content: ' ↑';
            opacity: 1;
            color: #fff;
        }
        
        th.sortable.sort-desc:after {
            content: ' ↓';
            opacity: 1;
            color: #fff;
        }
        
        td {
            padding: 15px;
            border-bottom: 1px solid #ecf0f1;
            font-size: 0.95em;
            vertical-align: middle;
        }
        
        tr:nth-child(even) {
            background-color: #f8f9fa;
        }
        
        tr:hover {
            background-color: #e3f2fd;
            transition: background-color 0.3s ease;
            transform: scale(1.002);
        }
        
        .metric-high {
            color: #27ae60;
            font-weight: 700;
        }
        
        .metric-medium {
            color: #f39c12;
            font-weight: 700;
        }
        
        .metric-low {
            color: #e74c3c;
            font-weight: 700;
        }
        
        .protocol-raw {
            background: linear-gradient(90deg, #2ecc71, #27ae60);
            color: white;
            padding: 6px 12px;
            border-radius: 6px;
            font-size: 0.8em;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .protocol-thrift {
            background: linear-gradient(90deg, #3498db, #2980b9);
            color: white;
            padding: 6px 12px;
            border-radius: 6px;
            font-size: 0.8em;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .protocol-grpc {
            background: linear-gradient(90deg, #9b59b6, #8e44ad);
            color: white;
            padding: 6px 12px;
            border-radius: 6px;
            font-size: 0.8em;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 25px;
            margin: 30px 0;
        }
        
        .stat-card {
            background: linear-gradient(135deg, #fff 0%, #f8f9fa 100%);
            padding: 25px;
            border-radius: 12px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.08);
            border-left: 5px solid #3498db;
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        
        .stat-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 8px 25px rgba(0,0,0,0.15);
        }
        
        .stat-card h4 {
            color: #2c3e50;
            margin-bottom: 15px;
            font-size: 1.2em;
            font-weight: 600;
        }
        
        .stat-value {
            font-size: 2.2em;
            font-weight: 700;
            color: #3498db;
            margin-bottom: 8px;
        }
        
        .stat-description {
            color: #7f8c8d;
            font-size: 0.9em;
        }
        
        .config-section {
            background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
            padding: 30px;
            border-radius: 10px;
            margin: 30px 0;
            border: 1px solid #dee2e6;
        }
        
        .comparison-section {
            background: linear-gradient(135deg, #fff3cd 0%, #ffeaa7 100%);
            padding: 30px;
            border-radius: 10px;
            margin: 30px 0;
            border-left: 5px solid #f39c12;
        }
        
        .footer {
            background: #2c3e50;
            color: white;
            text-align: center;
            padding: 30px;
            font-size: 0.95em;
        }
        
        .footer a {
            color: #3498db;
            text-decoration: none;
        }
        
        .alert {
            padding: 15px;
            margin: 20px 0;
            border-radius: 8px;
            border-left: 4px solid;
        }
        
        .alert-info {
            background-color: #d1ecf1;
            border-color: #17a2b8;
            color: #0c5460;
        }
        
        .alert-warning {
            background-color: #fff3cd;
            border-color: #ffc107;
            color: #856404;
        }
        
        @media (max-width: 768px) {
            .container {
                margin: 10px;
                border-radius: 10px;
            }
            
            .header {
                padding: 25px;
            }
            
            .header h1 {
                font-size: 2.2em;
            }
            
            .content {
                padding: 25px;
            }
            
            table {
                font-size: 0.85em;
            }
            
            th, td {
                padding: 10px 8px;
            }
            
            .stats-grid {
                grid-template-columns: 1fr;
            }
        }
        
        /* Print styles */
        @media print {
            body {
                background: white;
                padding: 0;
            }
            
            .container {
                box-shadow: none;
                border-radius: 0;
            }
            
            .header {
                background: #2c3e50 !important;
                -webkit-print-color-adjust: exact;
            }
        }
        """
    
    def generate_single_run_html(self, results: Dict[str, Any], output_dir: str) -> str:
        """Generate HTML report for a single benchmark run"""
        run_id = os.path.basename(output_dir)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Start building HTML
        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>KV Store Benchmark Report - {run_id}</title>
    <style>{self.generate_css()}</style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 KV Store Benchmark Report</h1>
            <div class="subtitle">Run ID: {run_id}</div>
            <div class="timestamp">Generated: {timestamp}</div>
        </div>
        
        <div class="content">
"""
        
        # Performance Summary Table
        html_content += self._generate_performance_summary(results)
        
        # Comparison Analysis
        html_content += self._generate_comparison_analysis(results)
        
        # Detailed Latency Analysis
        html_content += self._generate_latency_analysis(results)
        
        # Configuration Details
        html_content += self._generate_configuration_section(results)
        
        # Close HTML
        html_content += """
        </div>
        
        <div class="footer">
            <p>Generated by KV Store Benchmark Suite • Built with ❤️ for Performance Analysis</p>
            <p><small>For more information, visit the <a href="https://github.com/plusplusoneplusplus/kv">KV Store Repository</a></small></p>
        </div>
    </div>
    
    <script>
    // Table sorting functionality
    document.addEventListener('DOMContentLoaded', function() {
        const table = document.getElementById('performance-summary-table');
        const headers = table.querySelectorAll('th.sortable');
        
        // Helper functions
        function parseNumber(str) {
            if (str === 'N/A' || str === '') return -1;
            return parseFloat(str.replace(/[^0-9.-]/g, ''));
        }
        
        function parseDuration(str) {
            if (str === 'N/A' || str === '') return -1;
            const match = str.match(/([0-9.]+)([a-zA-Z]+)/);
            if (!match) return -1;
            
            const value = parseFloat(match[1]);
            const unit = match[2];
            
            // Convert to nanoseconds for consistent comparison
            switch(unit) {
                case 'ns': return value;
                case 'μs': return value * 1000;
                case 'ms': return value * 1000000;
                case 's': return value * 1000000000;
                default: return value;
            }
        }
        
        function sortTable(columnIndex, sortType, ascending = true) {
            const tbody = table.querySelector('tbody');
            const rows = Array.from(tbody.querySelectorAll('tr'));
            
            rows.sort((a, b) => {
                const aText = a.cells[columnIndex].textContent.trim();
                const bText = b.cells[columnIndex].textContent.trim();
                
                let aValue, bValue;
                
                switch(sortType) {
                    case 'number':
                        aValue = parseNumber(aText);
                        bValue = parseNumber(bText);
                        break;
                    case 'duration':
                        aValue = parseDuration(aText);
                        bValue = parseDuration(bText);
                        break;
                    case 'string':
                    default:
                        aValue = aText.toLowerCase();
                        bValue = bText.toLowerCase();
                        break;
                }
                
                if (aValue < bValue) return ascending ? -1 : 1;
                if (aValue > bValue) return ascending ? 1 : -1;
                return 0;
            });
            
            // Re-append rows in sorted order
            rows.forEach(row => tbody.appendChild(row));
        }
        
        // Add click listeners to sortable headers
        headers.forEach((header, index) => {
            header.addEventListener('click', function() {
                const sortType = this.getAttribute('data-sort');
                const isCurrentlyAsc = this.classList.contains('sort-asc');
                const isCurrentlyDesc = this.classList.contains('sort-desc');
                
                // Clear all sort indicators
                headers.forEach(h => h.classList.remove('sort-asc', 'sort-desc'));
                
                // Determine sort direction
                let ascending = true;
                if (isCurrentlyAsc) {
                    ascending = false;
                    this.classList.add('sort-desc');
                } else {
                    this.classList.add('sort-asc');
                }
                
                // Sort the table
                sortTable(index, sortType, ascending);
            });
        });
    });
    </script>
</body>
</html>"""
        
        return html_content
    
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
        
        html = f"""
            <div class="section">
                <h2>⚙️ Test Configuration</h2>
                <div class="config-section">
                    <div class="stats-grid">
                        <div class="stat-card">
                            <h4>Total Requests</h4>
                            <div class="stat-value">{self.format_number(config['total_requests'])}</div>
                            <div class="stat-description">per test case</div>
                        </div>
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
                            <div class="stat-description">total runtime</div>
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