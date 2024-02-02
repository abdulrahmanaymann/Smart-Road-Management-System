-- phpMyAdmin SQL Dump
-- version 5.2.0
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1
-- Generation Time: Dec 01, 2023 at 05:23 PM
-- Server version: 10.4.27-MariaDB
-- PHP Version: 8.2.0

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `py_test_db`
--

-- --------------------------------------------------------

--
-- Table structure for table `travels`
--

CREATE TABLE `travels` (
  `ID` varchar(255) DEFAULT NULL,
  `Start_Gate` varchar(255) DEFAULT NULL,
  `End_Gate` varchar(255) DEFAULT NULL,
  `Distance` int(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `travels`
--

INSERT INTO `travels` (`ID`, `Start_Gate`, `End_Gate`, `Distance`) VALUES
('110_Car', 'Aswan', 'Alexandria', 1000),
('711_Car', 'Giza', 'Cairo', 1),
('181_Bus', 'Giza', 'Cairo', 2),
('616_Car', 'Giza', 'Aswan', 700),
('717_Car', 'Giza', 'Aswan', 700),
('110_Bus', 'Giza', 'Cairo', 17),
('411_Car', 'Bus', 'Cairo', 2),
('981_Car', 'Cairo', 'Alexandria', 100),
('123_Car', 'giza', 'Aswan', 12),
('911_Bus', 'Giza', 'Cairo', 5),
('981_Car', 'Giza', 'Aswan', 700),
('911_Car', 'Cairo', 'Giza', 6),
('654_Bus', 'Giza', 'Aswan', 400),
('11_Car', 'Aswan', 'Giza', 6),
('115_Car', 'Giza', 'Aswan', 100),
('112_Car', 'Giza', 'Cairo', 6),
('112_Car', 'Giza', 'Cairo', 6),
('123_Car', 'Aswan', 'Giza', 100),
('123_Car', 'Cairo', 'Aswan', 2),
('999_Bus', 'Aswan', 'Luxor', 242),
('101_Bus', 'Cairo', 'Giza', 17),
('789_Motor', 'Aswan', 'Luxor', 242),
('900_Car', 'Cairo', 'Giza', 17),
('1000_Car', 'Cairo', 'Giza', 17),
('1001_Car', 'Cairo', 'Giza', 17),
('1200_Car', 'Aswan', 'Luxor', 242),
('1200_Car', 'Aswan', 'Luxor', 242),
('120_Car', 'Cairo', 'Giza', 17),
('111_Car', 'Aswan', 'Luxor', 242),
('111_Car', 'Aswan', 'Luxor', 242),
('131_Car', 'Cairo', 'Giza', 17),
('131_Car', 'Cairo', 'Giza', 17),
('131_Car', 'Aswan', 'Luxor', 242),
('222_Car', 'Aswan', 'Luxor', 242),
('222_Car', 'Cairo', 'Giza', 17),
('010_Motor', 'Luxor', 'Qena', 68),
('555_Car', 'Aswan', 'Luxor', 242),
('12345_Car', 'Aswan', 'Luxor', 242),
('432_Car', 'Giza', 'Cairo', 17),
('156_Car', 'Luxor', 'Qena', 68),
('000_Bus', 'Cairo', 'Giza', 17),
('777_Bus', 'Aswan', 'Luxor', 242),
('676_Bus', 'Cairo', 'Giza', 17),
('324_Car', 'Cairo', 'Giza', 17),
('12_car', 'Cairo', 'Giza', 17),
('321_Car', 'Cairo', 'Giza', 17),
('123_Bus', 'Luxor', 'Qena', 68),
('123_Car', 'Luxor', 'Qena', 68),
('321_Car', 'Aswan', 'Luxor', 242),
('111_Car', 'Aswan', 'Luxor', 242),
('122_Car', 'Aswan', 'Luxor', 242),
('122_Car', 'Aswan', 'Luxor', 242),
('122_Car', 'Luxor', 'Qena', 68),
('12111_Car', 'Cairo', 'Giza', 17),
('213_Bus', 'Cairo', 'Giza', 17),
('911_Car', 'Aswan', 'Luxor', 242),
('911_Car', 'Aswan', 'Luxor', 242);

-- --------------------------------------------------------

--
-- Table structure for table `violations`
--

CREATE TABLE `violations` (
  `Car_ID` varchar(255) DEFAULT NULL,
  `Start_Date` varchar(255) DEFAULT NULL,
  `End_Date` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `violations`
--

INSERT INTO `violations` (`Car_ID`, `Start_Date`, `End_Date`) VALUES
('290_Car', '2023-10-31 14:20:54', '2023-10-31 20:29:27'),
('181_Bus', '2023-11-01 11:45:43', '2023-11-01 11:46:31'),
('717_Car', '2023-11-01 13:07:35', '2023-11-01 13:08:04'),
('616_Car', '2023-11-01 13:07:35', '2023-11-01 13:11:41'),
('717_Car', '2023-11-01 13:07:35', '2023-11-01 13:12:19'),
('112_Car', '2023-11-01 13:43:14', '2023-11-01 13:44:19'),
('777_Bus', '2023-11-29 05:50:47', '2023-11-29 05:52:03');
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
