// Package pricing holds the AWS S3 cost tables s3du applies in the UI and
// CLI. Both list-request pricing (per 1 000 requests) and storage pricing
// (per GB-month, per storage class) are baked-in constants — refreshed
// manually from the AWS pricing API; they don't need to be exact, just
// close enough to give the operator a sense of the cost order of
// magnitude.
package pricing

// listPer1000 is S3 PUT/COPY/POST/LIST price per 1 000 requests, keyed by
// region. Unknown regions fall back to the us-east-1 baseline of $0.005.
// Source: AWS Bulk Pricing API, group=S3-API-Tier1, May 2026.
var listPer1000 = map[string]float64{
	// US
	"us-east-1": 0.005,
	"us-east-2": 0.005,
	"us-west-1": 0.0055,
	"us-west-2": 0.005,
	// Canada
	"ca-central-1": 0.0055,
	"ca-west-1":    0.0055,
	// Europe
	"eu-west-1":          0.005,
	"eu-west-2":          0.0053,
	"eu-west-3":          0.0053,
	"eu-central-1":       0.0054,
	"eu-central-2":       0.0054,
	"eu-central-1-ist-1": 0.0054,
	"eu-north-1":         0.005,
	"eu-south-1":         0.0053,
	"eu-south-2":         0.0053,
	// Asia Pacific
	"ap-northeast-1": 0.0047,
	"ap-northeast-2": 0.0045,
	"ap-northeast-3": 0.0047,
	"ap-southeast-1": 0.005,
	"ap-southeast-2": 0.0055,
	"ap-southeast-3": 0.005,
	"ap-southeast-4": 0.0055,
	"ap-southeast-5": 0.0045,
	"ap-southeast-6": 0.005775,
	"ap-southeast-7": 0.0045,
	"ap-south-1":     0.005,
	"ap-south-2":     0.005,
	"ap-east-1":      0.005,
	"ap-east-2":      0.00423,
	// Middle East
	"me-south-1":   0.0055,
	"me-central-1": 0.0055,
	// Israel
	"il-central-1": 0.0055,
	// Africa
	"af-south-1": 0.006,
	// South America
	"sa-east-1": 0.007,
	// Mexico
	"mx-central-1": 0.00525,
	// GovCloud
	"us-gov-east-1": 0.005,
	"us-gov-west-1": 0.005,
}

// ListPerRequest returns the per-request price for the given region.
// Always positive (small) — unknown regions fall back to $0.000005 / req.
func ListPerRequest(region string) float64 {
	rate, ok := listPer1000[region]
	if !ok {
		rate = 0.005
	}
	return rate / 1000
}

// ListCost is the convenience helper for "what did N list calls cost".
func ListCost(requests int64, region string) float64 {
	return float64(requests) * ListPerRequest(region)
}

// storagePerGBMonth is S3 storage price per GB per month, keyed by
// region/class. Falls back to us-east-1 pricing for unknown regions.
// Source: AWS S3 pricing tables, May 2026.
var storagePerGBMonth = map[string]float64{
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

// MonthlyStorage returns the per-month storage cost in USD for sizeBytes
// of an object of the given class in the given region. Unknown
// region/class combinations fall back to us-east-1 pricing; if even that
// is missing, 0 is returned.
func MonthlyStorage(sizeBytes int64, storageClass, region string) float64 {
	gb := float64(sizeBytes) / (1024 * 1024 * 1024)
	if price, ok := storagePerGBMonth[region+"/"+storageClass]; ok {
		return gb * price
	}
	if price, ok := storagePerGBMonth["us-east-1/"+storageClass]; ok {
		return gb * price
	}
	return 0
}
