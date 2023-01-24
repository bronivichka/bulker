package bulk

type BulkRecord struct {
	Bulk_id             int
	Bulk_type           string
	Active              bool
	File                string
	Subscription        int
	Subscription_filter string
	Message             string
	Start_stamp         string
	Finish_stamp        string
	Status              string
	Start_time          string
	End_time            string
	Load_limit          int
	Daily_limit         int
}

type VisitorRecord struct {
	Msisdn       int
	Date         string
	Subscription int
	Send_sms     bool
}

type VisitorReact struct {
	Msisdn       int
	Bulk_type    string
	Subscription int
	Action       string
	Days         int
	Bulk_id      int
}
