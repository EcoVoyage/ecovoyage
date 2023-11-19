group "default" {
  targets = ["base", "core", "jupyter", "ai", "spatial", "testing", "devel"]
}

target "base" {
  tags = ["ORG_PLACEHOLDER/base:DEFAULT_TAG_PLACEHOLDER", "ORG_PLACEHOLDER/base:DATE_TAG_PLACEHOLDER"]
  target = "base"
}

target "core" {
  tags = ["ORG_PLACEHOLDER/core:DEFAULT_TAG_PLACEHOLDER", "ORG_PLACEHOLDER/core:DATE_TAG_PLACEHOLDER"]
  target = "core"
  inherit = ["base"]
}

target "jupyter" {
  tags = ["ORG_PLACEHOLDER/jupyter:DEFAULT_TAG_PLACEHOLDER", "ORG_PLACEHOLDER/jupyter:DATE_TAG_PLACEHOLDER"]
  target = "jupyter"
  inherit = ["core"]
}

target "ai" {
  tags = ["ORG_PLACEHOLDER/ai:DEFAULT_TAG_PLACEHOLDER", "ORG_PLACEHOLDER/ai:DATE_TAG_PLACEHOLDER"]
  target = "ai"
  inherit = ["jupyter"]
}

target "spatial" {
  tags = ["ORG_PLACEHOLDER/spatial:DEFAULT_TAG_PLACEHOLDER", "ORG_PLACEHOLDER/spatial:DATE_TAG_PLACEHOLDER"]
  target = "spatial"
  inherit = ["ai"]
}

target "testing" {
  tags = ["ORG_PLACEHOLDER/testing:DEFAULT_TAG_PLACEHOLDER", "ORG_PLACEHOLDER/testing:DATE_TAG_PLACEHOLDER"]
  target = "testing"
  inherit = ["spatial"]
}

target "devel" {
  tags = ["ORG_PLACEHOLDER/devel:DEFAULT_TAG_PLACEHOLDER", "ORG_PLACEHOLDER/devel:DATE_TAG_PLACEHOLDER"]
  target = "devel"
  inherit = ["testing"]
}
