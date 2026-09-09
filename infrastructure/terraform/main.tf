terraform {
  required_version = ">= 1.10, < 2.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
}
variable "bucket_name" { type = string }
resource "aws_s3_bucket" "state" {
  bucket        = var.bucket_name
  force_destroy = false
  lifecycle { prevent_destroy = true }
}
resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}
resource "aws_s3_bucket_policy" "tls" {
  bucket = aws_s3_bucket.state.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Sid       = "DenyInsecureTransport", Effect = "Deny", Principal = "*", Action = "s3:*",
    Resource  = [aws_s3_bucket.state.arn, "${aws_s3_bucket.state.arn}/*"],
    Condition = { Bool = { "aws:SecureTransport" = "false" } }
  }] })
}
output "state_bucket" { value = aws_s3_bucket.state.id }
output "workload_identity_policy" {
  description = "Attach to the Flink workload identity; configure federation in the owning cluster module."
  value = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"], Resource = [aws_s3_bucket.state.arn] },
    { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"], Resource = [for p in ["checkpoints", "savepoints", "ha"] : "${aws_s3_bucket.state.arn}/${p}/*"] }
  ] })
}
