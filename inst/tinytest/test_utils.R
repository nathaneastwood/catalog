expect_silent(
  current = catalog:::check_character_one("hello")
)

expect_error(
  current = catalog:::check_character_one(1),
  pattern = "is.character\\(x = x\\) is not TRUE"
)

expect_error(
  current = catalog:::check_character_one(c("hello", "world")),
  pattern = "length\\(x = x\\) == 1L is not TRUE"
)
