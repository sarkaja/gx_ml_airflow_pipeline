from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class QualityThreshold:
    min_success_rate: float = 95.0
    max_null_percentage: float = 5.0
    min_data_volume: float = 0.8
    allowed_invalid_categories: int = 0
    
    def check_thresholds(self, dq_results: Dict[str, Any]) -> Dict[str, bool]:
        stats = dq_results.get('statistics', {})
        success_rate = stats.get('success_percent', 0)
        
        return {
            'success_rate_ok': success_rate >= self.min_success_rate,
            'completeness_ok': self._check_completeness(dq_results),
            'consistency_ok': self._check_consistency(dq_results),
            'volume_ok': self._check_volume(dq_results)
        }
    
    def _check_completeness(self, dq_results: Dict[str, Any]) -> bool:
        return True
    
    def _check_consistency(self, dq_results: Dict[str, Any]) -> bool:
        return True
    
    def _check_volume(self, dq_results: Dict[str, Any]) -> bool:
        return True