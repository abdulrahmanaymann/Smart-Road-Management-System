-- phpMyAdmin SQL Dump
-- version 5.2.0
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1
-- Generation Time: Feb 26, 2024 at 08:04 PM
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
('1110_Car', 'Cairo', 'Giza', 7),
('1110_Car', 'Qena', 'Luxor', 82),
('1110_Car', 'Qena', 'Luxor', 82),
('1110_Car', 'Luxor', 'Aswan', 194),
('1110_Car', 'Cairo', 'Giza', 7),
('1110_Car', 'Giza', 'Qalyubia', 32),
('1110_Car', 'Cairo', 'Giza', 7),
('1110_Taxi', 'Cairo', 'Giza', 7),
('1110_Taxi', 'Giza', 'Qalyubia', 32),
('1110_Taxi', 'Qalyubia', 'Monufia', 85),
('1110_Taxi', 'Qalyubia', 'Monufia', 85),
('1110_Taxi', 'Monufia', 'Gharbia', 44),
('1110_Taxi', 'Gharbia', 'Kafr El Sheikh', 52),
('1110_Taxi', 'Kafr El Sheikh', 'Damietta', 129),
('1110_Taxi', 'Damietta', 'Port Said', 94),
('1110_Taxi', 'Port Said', 'Minya', 444),
('1110_Taxi', 'Minya', 'Assiut', 187),
('1110_Taxi', 'Assiut', 'Sohag', 191),
('1110_Taxi', 'Sohag', 'Qena', 92),
('1110_Taxi', 'Giza', 'Qalyubia', 32),
('1110_Taxi', 'Luxor', 'Aswan', 194),
('1110_Taxi', 'Giza', 'Qalyubia', 32),
('1110_Taxi', 'Qena', 'Luxor', 82);

-- --------------------------------------------------------

--
-- Table structure for table `violations`
--

CREATE TABLE `violations` (
  `Car_ID` longtext DEFAULT NULL,
  `Start_Gate` varchar(255) DEFAULT NULL,
  `End_Gate` varchar(255) DEFAULT NULL,
  `Start_Date` longtext DEFAULT NULL,
  `End_Date` longtext DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `violations`
--

INSERT INTO `violations` (`Car_ID`, `Start_Gate`, `End_Gate`, `Start_Date`, `End_Date`) VALUES
('1110_Car', 'Cairo', 'Giza', '2024-02-26 00:25:36', '2024-02-26 01:58:45'),
('1110_Car', 'Qena', 'Luxor', '2024-02-26 01:59:47', '2024-02-26 02:00:10'),
('1110_Car', 'Luxor', 'Aswan', '2024-02-26 02:00:08', '2024-02-26 02:01:05'),
('1110_Car', 'Cairo', 'Giza', '2024-02-26 02:01:02', '2024-02-26 02:01:14'),
('1110_Car', 'Giza', 'Qalyubia', '2024-02-26 02:01:12', '2024-02-26 02:01:23'),
('1110_Taxi', 'Cairo', 'Giza', '2024-02-26 04:59:32', '2024-02-26 04:59:48'),
('1110_Taxi', 'Giza', 'Qalyubia', '2024-02-26 04:59:42', '2024-02-26 05:00:33'),
('1110_Taxi', 'Qalyubia', 'Monufia', '2024-02-26 16:50:04', '2024-02-26 16:50:23'),
('1110_Taxi', 'Monufia', 'Sharqia', '2024-02-26 16:50:18', '2024-02-26 16:50:37'),
('1110_Taxi', 'Gharbia', 'Beni Suef', '2024-02-26 16:50:32', '2024-02-26 16:50:49'),
('1110_Taxi', 'Kafr El Sheikh', 'Damietta', '2024-02-26 16:50:46', '2024-02-26 16:51:24'),
('1110_Taxi', 'Damietta', 'Alexandria', '2024-02-26 16:50:57', '2024-02-26 16:51:30'),
('1110_Taxi', 'Port Said', 'Minya', '2024-02-26 16:51:25', '2024-02-26 16:51:34'),
('1110_Taxi', 'Minya', 'Assiut', '2024-02-26 16:51:30', '2024-02-26 16:51:37'),
('1110_Taxi', 'Sohag', 'Qena', '2024-02-26 16:51:37', '2024-02-26 16:52:39'),
('1110_Taxi', 'Giza', 'Qalyubia', '2024-02-26 16:52:37', '2024-02-26 16:52:59'),
('1110_Taxi', 'Luxor', 'Aswan', '2024-02-26 16:52:56', '2024-02-26 16:53:14'),
('1110_Taxi', 'Giza', 'Qalyubia', '2024-02-26 16:53:11', '2024-02-26 16:53:26');
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
