package main

// listCostPer1000 is S3 PUT/COPY/POST/LIST price per 1,000 requests by region.
// Source: AWS Bulk Pricing API (pricing.us-east-1.amazonaws.com), group=S3-API-Tier1, May 2026.
// Default (unlisted/unknown regions): $0.005
var listCostPer1000 = map[string]float64{
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
	"eu-central-1-ist-1": 0.0054, // Istanbul Local Zone
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

func listCostForRegion(region string) float64 {
	if p, ok := listCostPer1000[region]; ok {
		return p
	}
	return 0.005
}

func computeCost(listRequests int64, region string) float64 {
	pricePerReq := listCostForRegion(region) / 1000.0
	return float64(listRequests) * pricePerReq
}
