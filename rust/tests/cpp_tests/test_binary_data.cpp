#include <gtest/gtest.h>
#include "test_common.hpp"

class BinaryDataTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(BinaryDataTest, PlaceholderTest) {
    EXPECT_TRUE(true) << "Placeholder test - needs conversion";
}