package main

// storageCostPerGBMonth is S3 storage price per GB per month.
// Source: AWS S3 pricing (us-east-1 baseline), May 2026.
// Key format: "region/storageClass". Falls back to "default/storageClass", then 0.
var storageCostPerGBMonth = map[string]float64{
	// US East (N. Virginia) — baseline for most classes
	"us-east-1/STANDARD":            0.023,
	"us-east-1/STANDARD_IA":         0.0125,
	"us-east-1/ONEZONE_IA":          0.01,
	"us-east-1/INTELLIGENT_TIERING": 0.023,
	"us-east-1/GLACIER_IR":          0.004,
	"us-east-1/GLACIER":             0.0036,
	"us-east-1/DEEP_ARCHIVE":        0.00099,
	"us-east-1/REDUCED_REDUNDANCY":  0.024,
	// US East (Ohio)
	"us-east-2/STANDARD":     0.023,
	"us-east-2/STANDARD_IA":  0.0125,
	"us-east-2/ONEZONE_IA":   0.01,
	"us-east-2/GLACIER_IR":   0.004,
	"us-east-2/GLACIER":      0.0036,
	"us-east-2/DEEP_ARCHIVE": 0.00099,
	// US West (N. California)
	"us-west-1/STANDARD":     0.026,
	"us-west-1/STANDARD_IA":  0.0138,
	"us-west-1/ONEZONE_IA":   0.011,
	"us-west-1/GLACIER_IR":   0.0044,
	"us-west-1/GLACIER":      0.004,
	"us-west-1/DEEP_ARCHIVE": 0.003,
	// US West (Oregon)
	"us-west-2/STANDARD":     0.023,
	"us-west-2/STANDARD_IA":  0.0125,
	"us-west-2/ONEZONE_IA":   0.01,
	"us-west-2/GLACIER_IR":   0.004,
	"us-west-2/GLACIER":      0.0036,
	"us-west-2/DEEP_ARCHIVE": 0.00099,
	// Europe (Ireland)
	"eu-west-1/STANDARD":     0.023,
	"eu-west-1/STANDARD_IA":  0.0125,
	"eu-west-1/ONEZONE_IA":   0.01,
	"eu-west-1/GLACIER_IR":   0.004,
	"eu-west-1/GLACIER":      0.0036,
	"eu-west-1/DEEP_ARCHIVE": 0.00099,
	// Europe (Frankfurt)
	"eu-central-1/STANDARD":     0.0245,
	"eu-central-1/STANDARD_IA":  0.0131,
	"eu-central-1/ONEZONE_IA":   0.0105,
	"eu-central-1/GLACIER_IR":   0.0042,
	"eu-central-1/GLACIER":      0.0038,
	"eu-central-1/DEEP_ARCHIVE": 0.001,
	// AP Southeast (Singapore)
	"ap-southeast-1/STANDARD":     0.025,
	"ap-southeast-1/STANDARD_IA":  0.0138,
	"ap-southeast-1/ONEZONE_IA":   0.011,
	"ap-southeast-1/GLACIER_IR":   0.0043,
	"ap-southeast-1/GLACIER":      0.0039,
	"ap-southeast-1/DEEP_ARCHIVE": 0.001,
	// AP Northeast (Tokyo)
	"ap-northeast-1/STANDARD":     0.025,
	"ap-northeast-1/STANDARD_IA":  0.0138,
	"ap-northeast-1/ONEZONE_IA":   0.011,
	"ap-northeast-1/GLACIER_IR":   0.0043,
	"ap-northeast-1/GLACIER":      0.0039,
	"ap-northeast-1/DEEP_ARCHIVE": 0.001,
	// South America (São Paulo)
	"sa-east-1/STANDARD":     0.0405,
	"sa-east-1/STANDARD_IA":  0.0228,
	"sa-east-1/ONEZONE_IA":   0.0182,
	"sa-east-1/GLACIER_IR":   0.007,
	"sa-east-1/GLACIER":      0.0064,
	"sa-east-1/DEEP_ARCHIVE": 0.00228,
}

func monthlyStorageCost(sizeBytes int64, storageClass, region string) float64 {
	key := region + "/" + storageClass
	if price, ok := storageCostPerGBMonth[key]; ok {
		return float64(sizeBytes) / (1024 * 1024 * 1024) * price
	}
	// fall back to us-east-1 pricing for unknown regions
	key = "us-east-1/" + storageClass
	if price, ok := storageCostPerGBMonth[key]; ok {
		return float64(sizeBytes) / (1024 * 1024 * 1024) * price
	}
	return 0
}
